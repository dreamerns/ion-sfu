package sfu

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/transport/packetio"
	"github.com/pion/webrtc/v3"

	"github.com/pion/ion-sfu/pkg/buffer"
)

// DownTrackType determines the type of track
type DownTrackType int

const (
	SimpleDownTrack DownTrackType = iota + 1
	SimulcastDownTrack
)

// DownTrack  implements TrackLocal, is the track used to write packets
// to SFU Subscriber, the track handle the packets for simple, simulcast
// and SVC Publisher.
type DownTrack struct {
	id            string
	peerID        string
	bound         atomicBool
	mime          string
	ssrc          uint32
	streamID      string
	maxTrack      int
	payloadType   uint8
	sequencer     *sequencer
	trackType     DownTrackType
	bufferFactory *buffer.Factory
	payload       *[]byte

	currentSpatialLayer atomicInt32
	targetSpatialLayer  atomicInt32
	temporalLayer       atomicInt32

	enabled  atomicBool
	reSync   atomicBool
	lastSSRC atomicUint32
	snOffset atomicUint16
	lastSN   atomicUint16
	tsOffset atomicUint32
	lastTS   atomicUint32
	snOffset uint16
	tsOffset uint32

	simulcast        simulcastTrackHelpers
	maxSpatialLayer  atomicInt32
	maxTemporalLayer atomicInt32

	codec          webrtc.RTPCodecCapability
	receiver       Receiver
	transceiver    *webrtc.RTPTransceiver
	writeStream    webrtc.TrackLocalWriter
	onCloseHandler func()
	onBind         func()
	closeOnce      sync.Once

	// Report helpers
	octetCount   atomicUint32
	packetCount  atomicUint32
	maxPacketTs  atomicUint32
	lastPacketMs atomicInt64

	// Debug info
	lastPli     atomicInt64
	lastRTP     atomicInt64
	pktsMuted   atomicUint32
	pktsDropped atomicUint32
}

// NewDownTrack returns a DownTrack.
func NewDownTrack(c webrtc.RTPCodecCapability, r Receiver, bf *buffer.Factory, peerID string, mt int) (*DownTrack, error) {
	return &DownTrack{
		id:            r.TrackID(),
		peerID:        peerID,
		maxTrack:      mt,
		streamID:      r.StreamID(),
		bufferFactory: bf,
		receiver:      r,
		codec:         c,
	}, nil
}

// Bind is called by the PeerConnection after negotiation is complete
// This asserts that the code requested is supported by the remote peer.
// If so it setups all the state (SSRC and PayloadType) to have a call
func (d *DownTrack) Bind(t webrtc.TrackLocalContext) (webrtc.RTPCodecParameters, error) {
	parameters := webrtc.RTPCodecParameters{RTPCodecCapability: d.codec}
	if codec, err := codecParametersFuzzySearch(parameters, t.CodecParameters()); err == nil {
		d.ssrc = uint32(t.SSRC())
		d.payloadType = uint8(codec.PayloadType)
		d.writeStream = t.WriteStream()
		d.mime = strings.ToLower(codec.MimeType)
		d.reSync.set(true)
		d.enabled.set(true)
		if rr := d.bufferFactory.GetOrNew(packetio.RTCPBufferPacket, uint32(t.SSRC())).(*buffer.RTCPReader); rr != nil {
			rr.OnPacket(func(pkt []byte) {
				d.handleRTCP(pkt)
			})
		}
		if strings.HasPrefix(d.codec.MimeType, "video/") {
			d.sequencer = newSequencer(d.maxTrack)
		}
		if d.onBind != nil {
			d.onBind()
		}
		d.bound.set(true)
		return codec, nil
	}
	return webrtc.RTPCodecParameters{}, webrtc.ErrUnsupportedCodec
}

// Unbind implements the teardown logic when the track is no longer needed. This happens
// because a track has been stopped.
func (d *DownTrack) Unbind(_ webrtc.TrackLocalContext) error {
	d.bound.set(false)
	d.receiver.DeleteDownTrack(d.peerID)
	return nil
}

// ID is the unique identifier for this Track. This should be unique for the
// stream, but doesn't have to globally unique. A common example would be 'audio' or 'video'
// and StreamID would be 'desktop' or 'webcam'
func (d *DownTrack) ID() string { return d.id }

// Codec returns current track codec capability
func (d *DownTrack) Codec() webrtc.RTPCodecCapability { return d.codec }

// StreamID is the group this track belongs too. This must be unique
func (d *DownTrack) StreamID() string { return d.streamID }

// Kind controls if this TrackLocal is audio or video
func (d *DownTrack) Kind() webrtc.RTPCodecType {
	switch {
	case strings.HasPrefix(d.codec.MimeType, "audio/"):
		return webrtc.RTPCodecTypeAudio
	case strings.HasPrefix(d.codec.MimeType, "video/"):
		return webrtc.RTPCodecTypeVideo
	default:
		return webrtc.RTPCodecType(0)
	}
}

func (d *DownTrack) Stop() error {
	if d.transceiver != nil {
		return d.transceiver.Stop()
	}
	return fmt.Errorf("d.transceiver not exists")
}

func (d *DownTrack) SetTransceiver(transceiver *webrtc.RTPTransceiver) {
	d.transceiver = transceiver
}

// WriteRTP writes a RTP Packet to the DownTrack
func (d *DownTrack) WriteRTP(p *buffer.ExtPacket, layer int32) error {
	d.lastRTP.set(time.Now().UnixNano())

	if !d.bound.get() {
		return nil
	}
	if !d.enabled.get() {
		d.pktsMuted.add(1)
		return nil
	}

	switch d.trackType {
	case SimpleDownTrack:
		return d.writeSimpleRTP(p)
	case SimulcastDownTrack:
		return d.writeSimulcastRTP(p, layer)
	}
	return nil
}

func (d *DownTrack) Enabled() bool {
	return d.enabled.get()
}

// Mute enables or disables media forwarding
func (d *DownTrack) Mute(val bool) {
	if d.enabled.get() != val {
		return
	}
	d.enabled.set(!val)
	if val {
		d.reSync.set(val)
	}
}

// Close track
func (d *DownTrack) Close() {
	d.enabled.set(false)
	d.closeOnce.Do(func() {
		Logger.V(1).Info("Closing sender", "peer_id", d.peerID)
		if d.payload != nil {
			packetFactory.Put(d.payload)
		}
		if d.onCloseHandler != nil {
			d.onCloseHandler()
		}
	})
}

func (d *DownTrack) SetInitialLayers(spatialLayer, temporalLayer int32) {
	d.currentSpatialLayer.set(spatialLayer)
	d.targetSpatialLayer.set(spatialLayer)
	d.temporalLayer.set((temporalLayer << 16) | temporalLayer)
}

func (d *DownTrack) CurrentSpatialLayer() int32 {
	return d.currentSpatialLayer.get()
}

func (d *DownTrack) TargetSpatialLayer() int32 {
	return d.targetSpatialLayer.get()
}

// SwitchSpatialLayer switches the current layer
func (d *DownTrack) SwitchSpatialLayer(targetLayer int32, setAsMax bool) error {
	if d.trackType != SimulcastDownTrack {
		return ErrSpatialNotSupported
	}
	if !d.receiver.HasSpatialLayer(targetLayer) {
		return ErrSpatialLayerNotFound
	}
	// already set
	if d.CurrentSpatialLayer() == targetLayer {
		return nil
	}

	d.targetSpatialLayer.set(targetLayer)
	if setAsMax {
		d.maxSpatialLayer.set(targetLayer)
	}
	return nil
}

func (d *DownTrack) SwitchSpatialLayerDone(layer int32) {
	d.currentSpatialLayer.set(layer)
}

func (d *DownTrack) UptrackLayersChange(availableLayers []uint16) (int32, error) {
	if d.trackType == SimulcastDownTrack {
		currentLayer := uint16(d.CurrentSpatialLayer())
		maxLayer := uint16(d.maxSpatialLayer.get())

		var maxFound uint16 = 0
		layerFound := false
		var minFound uint16 = 0
		for _, target := range availableLayers {
			if target <= maxLayer {
				if target > maxFound {
					maxFound = target
					layerFound = true
				}
			} else {
				if minFound > target {
					minFound = target
				}
			}
		}
		var targetLayer uint16
		if layerFound {
			targetLayer = maxFound
		} else {
			targetLayer = minFound
		}
		if currentLayer != targetLayer {
			if err := d.SwitchSpatialLayer(int32(targetLayer), false); err != nil {
				return int32(targetLayer), err
			}
		}
		return int32(targetLayer), nil
	}
	return -1, fmt.Errorf("downtrack %s does not support simulcast", d.id)
}

func (d *DownTrack) SwitchTemporalLayer(targetLayer int32, setAsMax bool) {
	if d.trackType == SimulcastDownTrack {
		layer := d.temporalLayer.get()
		currentLayer := uint16(layer)
		currentTargetLayer := uint16(layer >> 16)

		// Don't switch until previous switch is done or canceled
		if currentLayer != currentTargetLayer {
			return
		}
		d.temporalLayer.set((targetLayer << 16) | int32(currentLayer))
		if setAsMax {
			d.maxTemporalLayer.set(targetLayer)
		}
	}
}

// OnCloseHandler method to be called on remote tracked removed
func (d *DownTrack) OnCloseHandler(fn func()) {
	d.onCloseHandler = fn
}

func (d *DownTrack) OnBind(fn func()) {
	d.onBind = fn
}

func (d *DownTrack) CreateSourceDescriptionChunks() []rtcp.SourceDescriptionChunk {
	if !d.bound.get() {
		return nil
	}
	return []rtcp.SourceDescriptionChunk{
		{
			Source: d.ssrc,
			Items: []rtcp.SourceDescriptionItem{{
				Type: rtcp.SDESCNAME,
				Text: d.streamID,
			}},
		}, {
			Source: d.ssrc,
			Items: []rtcp.SourceDescriptionItem{{
				Type: rtcp.SDESType(15),
				Text: d.transceiver.Mid(),
			}},
		},
	}
}

func (d *DownTrack) CreateSenderReport() *rtcp.SenderReport {
	if !d.bound.get() {
		return nil
	}

	srRTP, srNTP := d.receiver.GetSenderReportTime(d.CurrentSpatialLayer())
	if srRTP == 0 {
		return nil
	}

	now := time.Now()
	nowNTP := toNtpTime(now)

	diff := (uint64(now.Sub(ntpTime(srNTP).Time())) * uint64(d.codec.ClockRate)) / uint64(time.Second)
	if diff < 0 {
		diff = 0
	}

	return &rtcp.SenderReport{
		SSRC:        d.ssrc,
		NTPTime:     uint64(nowNTP),
		RTPTime:     srRTP + uint32(diff),
		PacketCount: d.packetCount.get(),
		OctetCount:  d.octetCount.get(),
	}
}

func (d *DownTrack) UpdateStats(packetLen uint32) {
	d.octetCount.add(packetLen)
	d.packetCount.add(1)
}

func (d *DownTrack) writeSimpleRTP(extPkt *buffer.ExtPacket) error {
	if d.reSync.get() {
		if d.Kind() == webrtc.RTPCodecTypeVideo {
			if !extPkt.KeyFrame {
				d.receiver.SendRTCP([]rtcp.Packet{
					&rtcp.PictureLossIndication{SenderSSRC: d.ssrc, MediaSSRC: extPkt.Packet.SSRC},
				})
				d.lastPli.set(time.Now().UnixNano())
				d.pktsDropped.add(1)
				return nil
			}
		}
		if d.lastSN != 0 {
			d.snOffset.set(extPkt.Packet.SequenceNumber - d.lastSN.get() - 1)
			d.tsOffset.set(extPkt.Packet.Timestamp - d.lastTS.get() - 1)
		}
		d.lastSSRC.set(extPkt.Packet.SSRC)
		d.reSync.set(false)
	}

	d.UpdateStats(uint32(len(extPkt.Packet.Payload)))

	newSN := extPkt.Packet.SequenceNumber - d.snOffset.get()
	newTS := extPkt.Packet.Timestamp - d.tsOffset.get()

	if d.sequencer != nil {
		d.sequencer.push(extPkt.Packet.SequenceNumber, newSN, newTS, 0, extPkt.Head)
	}
	if extPkt.Head {
		d.lastSN.set(newSN)
		d.lastTS.set(newTS)
	}

	hdr := extPkt.Packet.Header
	hdr.PayloadType = d.payloadType
	hdr.Timestamp = newTS
	hdr.SequenceNumber = newSN
	hdr.SSRC = d.ssrc

	_, err := d.writeStream.WriteRTP(&hdr, extPkt.Packet.Payload)
	return err
}

func (d *DownTrack) writeSimulcastRTP(extPkt *buffer.ExtPacket, layer int32) error {
	// Check if packet SSRC is different from before
	// if true, the video source changed
	reSync := d.reSync.get()
	csl := d.CurrentSpatialLayer()

	if csl != layer {
		return nil
	}

	lastSSRC := d.lastSSRC.get()
	if lastSSRC != extPkt.Packet.SSRC || reSync {
		// Wait for a keyframe to sync new source
		if reSync && !extPkt.KeyFrame {
			// Packet is not a keyframe, discard it
			d.receiver.SendRTCP([]rtcp.Packet{
				&rtcp.PictureLossIndication{SenderSSRC: d.ssrc, MediaSSRC: extPkt.Packet.SSRC},
			})
			d.lastPli.set(time.Now().UnixNano())
			d.pktsDropped.add(1)
			return nil
		}
		if reSync && d.simulcast.lTSCalc.get() != 0 {
			d.simulcast.lTSCalc.set(extPkt.Arrival)
		}

		if d.simulcast.temporalSupported {
			if d.mime == "video/vp8" {
				if vp8, ok := extPkt.Payload.(buffer.VP8); ok {
					d.simulcast.pRefPicID.set(d.simulcast.lPicID.get())
					d.simulcast.refPicID.set(vp8.PictureID)
					d.simulcast.pRefTlZIdx.set(d.simulcast.lTlZIdx.get())
					d.simulcast.refTlZIdx.set(vp8.TL0PICIDX)
				}
			}
		}
		d.reSync.set(false)
	}
	// Compute how much time passed between the old RTP extPkt
	// and the current packet, and fix timestamp on source change
	lTSCalc := d.simulcast.lTSCalc.get()
	if lTSCalc != 0 && lastSSRC != extPkt.Packet.SSRC {
		d.lastSSRC.set(extPkt.Packet.SSRC)
		tDiff := (extPkt.Arrival - lTSCalc) / 1e6
		td := uint32((tDiff * 90) / 1000)
		if td == 0 {
			td = 1
		}
		d.tsOffset.set(extPkt.Packet.Timestamp - (d.lastTS.get() + td))
		d.snOffset.set(extPkt.Packet.SequenceNumber - d.lastSN.get() - 1)
	} else if lTSCalc == 0 {
		d.lastTS.set(extPkt.Packet.Timestamp)
		d.lastSN.set(extPkt.Packet.SequenceNumber)
		if d.mime == "video/vp8" {
			if vp8, ok := extPkt.Payload.(buffer.VP8); ok {
				d.simulcast.temporalSupported = vp8.TemporalSupported
			}
		}
	}

	newSN := extPkt.Packet.SequenceNumber - d.snOffset.get()
	newTS := extPkt.Packet.Timestamp - d.tsOffset.get()
	payload := extPkt.Packet.Payload

	var (
		picID   uint16
		tlz0Idx uint8
	)
	if d.simulcast.temporalSupported {
		if d.mime == "video/vp8" {
			drop := false
			if payload, picID, tlz0Idx, drop = setVP8TemporalLayer(extPkt, d); drop {
				// Pkt not in temporal getLayer update sequence number offset to avoid gaps
				d.snOffset.add(1)
				return nil
			}
		}
	}

	if d.sequencer != nil {
		if meta := d.sequencer.push(extPkt.Packet.SequenceNumber, newSN, newTS, uint8(csl), extPkt.Head); meta != nil &&
			d.simulcast.temporalSupported && d.mime == "video/vp8" {
			meta.setVP8PayloadMeta(tlz0Idx, picID)
		}
	}

	d.UpdateStats(uint32(len(extPkt.Packet.Payload)))

	if extPkt.Head {
		d.lastSN.set(newSN)
		d.lastTS.set(newTS)
		d.lastPacketMs.set(time.Now().UnixNano() / 1e6)
	}

	// Update base
	d.simulcast.lTSCalc.set(extPkt.Arrival)
	// Update extPkt headers
	hdr := extPkt.Packet.Header
	hdr.SequenceNumber = newSN
	hdr.Timestamp = newTS
	hdr.SSRC = d.ssrc
	hdr.PayloadType = d.payloadType

	_, err := d.writeStream.WriteRTP(&hdr, payload)
	return err
}

func (d *DownTrack) handleRTCP(bytes []byte) {
	if !d.enabled.get() {
		return
	}

	pkts, err := rtcp.Unmarshal(bytes)
	if err != nil {
		Logger.Error(err, "Unmarshal rtcp receiver packets err")
	}

	var fwdPkts []rtcp.Packet
	pliOnce := true
	firOnce := true

	var (
		maxRatePacketLoss  uint8
		expectedMinBitrate uint64
	)

	ssrc := d.lastSSRC.get()
	if ssrc == 0 {
		return
	}
	for _, pkt := range pkts {
		switch p := pkt.(type) {
		case *rtcp.PictureLossIndication:
			if pliOnce {
				d.lastPli.set(time.Now().UnixNano())
				p.MediaSSRC = ssrc
				p.SenderSSRC = d.ssrc
				fwdPkts = append(fwdPkts, p)
				pliOnce = false
			}
		case *rtcp.FullIntraRequest:
			if firOnce {
				p.MediaSSRC = ssrc
				p.SenderSSRC = d.ssrc
				fwdPkts = append(fwdPkts, p)
				firOnce = false
			}
		case *rtcp.ReceiverEstimatedMaximumBitrate:
			if expectedMinBitrate == 0 || expectedMinBitrate > p.Bitrate {
				expectedMinBitrate = p.Bitrate
			}
		case *rtcp.ReceiverReport:
			for _, r := range p.Reports {
				if maxRatePacketLoss == 0 || maxRatePacketLoss < r.FractionLost {
					maxRatePacketLoss = r.FractionLost
				}
			}
		case *rtcp.TransportLayerNack:
			var nackedPackets []packetMeta
			for _, pair := range p.Nacks {
				nackedPackets = append(nackedPackets, d.sequencer.getSeqNoPairs(pair.PacketList())...)
			}
			if err = d.receiver.RetransmitPackets(d, nackedPackets); err != nil {
				return
			}
		}
	}
	if d.trackType == SimulcastDownTrack && (maxRatePacketLoss != 0 || expectedMinBitrate != 0) {
		d.handleLayerChange(maxRatePacketLoss, expectedMinBitrate)
	}

	if len(fwdPkts) > 0 {
		d.receiver.SendRTCP(fwdPkts)
	}
}

func (d *DownTrack) handleLayerChange(maxRatePacketLoss uint8, expectedMinBitrate uint64) {
	currentSpatialLayer := d.CurrentSpatialLayer()
	targetSpatialLayer := d.TargetSpatialLayer()

	temporalLayer := d.temporalLayer.get()
	currentTemporalLayer := temporalLayer & 0x0f
	targetTemporalLayer := temporalLayer >> 16

	if targetSpatialLayer == currentSpatialLayer && currentTemporalLayer == targetTemporalLayer {
		if time.Now().After(d.simulcast.switchDelay) {
			brs := d.receiver.GetBitrate()
			cbr := brs[currentSpatialLayer]
			mtl := d.receiver.GetMaxTemporalLayer()
			mctl := mtl[currentSpatialLayer]

			if maxRatePacketLoss <= 5 {
				if currentTemporalLayer < mctl && currentTemporalLayer+1 <= d.maxTemporalLayer.get() &&
					expectedMinBitrate >= 3*cbr/4 {
					d.SwitchTemporalLayer(currentTemporalLayer+1, false)
					d.simulcast.switchDelay = time.Now().Add(3 * time.Second)
				}
				if currentTemporalLayer >= mctl && expectedMinBitrate >= 3*cbr/2 && currentSpatialLayer+1 <= d.maxSpatialLayer.get() &&
					currentSpatialLayer+1 <= 2 {
					if err := d.SwitchSpatialLayer(currentSpatialLayer+1, false); err == nil {
						d.SwitchTemporalLayer(0, false)
					}
					d.simulcast.switchDelay = time.Now().Add(5 * time.Second)
				}
			}
			if maxRatePacketLoss >= 25 {
				if (expectedMinBitrate <= 5*cbr/8 || currentTemporalLayer == 0) &&
					currentSpatialLayer > 0 &&
					brs[currentSpatialLayer-1] != 0 {
					if err := d.SwitchSpatialLayer(currentSpatialLayer-1, false); err != nil {
						d.SwitchTemporalLayer(mtl[currentSpatialLayer-1], false)
					}
					d.simulcast.switchDelay = time.Now().Add(10 * time.Second)
				} else {
					d.SwitchTemporalLayer(currentTemporalLayer-1, false)
					d.simulcast.switchDelay = time.Now().Add(5 * time.Second)
				}
			}
		}
	}
}

func (d *DownTrack) DebugInfo() map[string]interface{} {
	stats := map[string]interface{}{
		"LastSN":         d.lastSN.get(),
		"SNOffset":       d.snOffset.get(),
		"LastTS":         d.lastTS.get(),
		"TSOffset":       d.tsOffset.get(),
		"LastRTP":        d.lastRTP.get(),
		"LastPli":        d.lastPli.get(),
		"PacketsDropped": d.pktsDropped.get(),
		"PacketsMuted":   d.pktsMuted.get(),
	}

	senderReport := d.CreateSenderReport()
	if senderReport != nil {
		stats["NTPTime"] = senderReport.NTPTime
		stats["RTPTime"] = senderReport.RTPTime
		stats["PacketCount"] = senderReport.PacketCount
	}

	return map[string]interface{}{
		"PeerID":              d.peerID,
		"TrackID":             d.id,
		"StreamID":            d.streamID,
		"SSRC":                d.ssrc,
		"MimeType":            d.codec.MimeType,
		"Bound":               d.bound.get(),
		"Enabled":             d.enabled.get(),
		"Resync":              d.reSync.get(),
		"CurrentSpatialLayer": d.CurrentSpatialLayer(),
		"Stats":               stats,
	}
}
