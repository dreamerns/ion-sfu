package sfu

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/pion/rtcp"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
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

const (
	RTPPaddingMaxPayloadSize      = 255
	RTPPaddingEstimatedHeaderSize = 20
)

var (
	ErrPaddingNotOnFrameBoundary = errors.New("padding cannot send on non-frame boundary")
)

type ReceiverReportListener func(dt *DownTrack, report *rtcp.ReceiverReport)

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

	munger Munger

	simulcast        simulcastTrackHelpers
	maxSpatialLayer  atomicInt32
	maxTemporalLayer atomicInt32

	codec                   webrtc.RTPCodecCapability
	rtpHeaderExtensions     []webrtc.RTPHeaderExtensionParameter
	receiver                Receiver
	transceiver             *webrtc.RTPTransceiver
	writeStream             webrtc.TrackLocalWriter
	onCloseHandler          func()
	onBind                  func()
	receiverReportListeners []ReceiverReportListener
	listenerLock            sync.RWMutex
	closeOnce               sync.Once

	// Report helpers
	octetCount   atomicUint32
	packetCount  atomicUint32
	lossFraction atomicUint8

	// Debug info
	lastPli                         atomicInt64
	lastRTP                         atomicInt64
	pktsMuted                       atomicUint32
	pktsDropped                     atomicUint32
	pktsBandwidthConstrainedDropped atomicUint32

	maxPacketTs uint32

	bandwidthConstrainedMuted atomicBool

	// RTCP callbacks
	onREMB func(dt *DownTrack, remb *rtcp.ReceiverEstimatedMaximumBitrate)

	// simulcast layer availability change callback
	onAvailableLayersChanged func(dt *DownTrack, layerAdded bool)

	// subscription change callback
	onSubscriptionChanged func(dt *DownTrack)

	// packet sent callback
	onPacketSent func(dt *DownTrack, size int)
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
		d.bandwidthConstrainedMute(false)
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

// Sets RTP header extensions for this track
func (d *DownTrack) SetRTPHeaderExtensions(rtpHeaderExtensions []webrtc.RTPHeaderExtensionParameter) {
	d.rtpHeaderExtensions = rtpHeaderExtensions
}

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

func (d *DownTrack) SSRC() uint32 {
	return d.ssrc
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

// Writes RTP header extensions of track
func (d *DownTrack) WriteRTPHeaderExtensions(hdr *rtp.Header) error {
	// clear out extensions that may have been in the forwarded header
	hdr.Extension = false
	hdr.ExtensionProfile = 0
	hdr.Extensions = []rtp.Extension{}

	for _, ext := range d.rtpHeaderExtensions {
		if ext.URI != sdp.ABSSendTimeURI {
			// supporting only abs-send-time
			continue
		}

		sendTime := rtp.NewAbsSendTimeExtension(time.Now())
		b, err := sendTime.Marshal()
		if err != nil {
			return err
		}

		err = hdr.SetExtension(uint8(ext.ID), b)
		if err != nil {
			return err
		}
	}

	return nil
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

	if d.bandwidthConstrainedMuted.get() {
		d.pktsBandwidthConstrainedDropped.add(1)
		return nil
	}

	// drop padding only forwarded packet
	if len(p.Packet.Payload) == 0 && d.packetCount.get() > 0 {
		d.munger.addSnOffset(1)
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

// WritePaddingRTP tries to write as many padding only RTP packets as necessary
// to satisfy given size to the DownTrack
func (d *DownTrack) WritePaddingRTP(bytesToSend int) int {
	// LK-TODO-START
	// Potentially write padding even if muted. Given that padding
	// can be sent only on frame boudaries, writing on disabled tracks
	// will give more options. But, it is possible that forwarding stopped
	// on a non-frame boundary when the track is muted.
	// LK-TODO-END
	if !d.enabled.get() || d.packetCount.get() == 0 {
		return 0
	}

	// LK-TODO-START
	// Ideally should look at header extensions negotiated for
	// track and decide if padding can be sent. But, browsers behave
	// in unexpected ways when using audio for bandwidth estimation and
	// padding is mainly used to probe for excess available bandwidth.
	// So, to be safe, limit to video tracks
	// LK-TODO-END
	if d.Kind() == webrtc.RTPCodecTypeAudio {
		return 0
	}

	// LK-TODO Look at load balancing a la sfu.Receiver to spread across available CPUs
	bytesSent := 0
	for {
		size := bytesToSend
		// RTP padding maximum is 255 bytes. Break it up.
		// Use 20 byte as estimate of RTP header size (12 byte header + 8 byte extension)
		if size > RTPPaddingMaxPayloadSize+RTPPaddingEstimatedHeaderSize {
			size = RTPPaddingMaxPayloadSize + RTPPaddingEstimatedHeaderSize
		}

		sn, ts, err := d.munger.updateAndGetPaddingSnTs()
		if err != nil {
			return bytesSent
		}

		// LK-TODO-START
		// Hold sending padding packets till first RTCP-RR is received for this RTP stream.
		// That is definitive proof that the remote side knows about this RTP stream.
		// The packet count check at the beginning of this function gates sending padding
		// on as yet unstarted streams which is a reasonble check.
		// LK-TODO-END

		// intentionally ignoring check for bandwidth constrained mute
		// as padding is typically used to probe for channel capacity
		// and sending it on any track achieves the purpose of probing
		// the channel.

		hdr := rtp.Header{
			Version:        2,
			Padding:        true,
			Marker:         false,
			PayloadType:    d.payloadType,
			SequenceNumber: sn,
			Timestamp:      ts,
			SSRC:           d.ssrc,
			CSRC:           []uint32{},
		}

		err = d.WriteRTPHeaderExtensions(&hdr)
		if err != nil {
			return bytesSent
		}

		payloadSize := size - RTPPaddingEstimatedHeaderSize
		payload := make([]byte, payloadSize)
		// last byte of padding has padding size including that byte
		payload[payloadSize-1] = byte(payloadSize)

		_, err = d.writeStream.WriteRTP(&hdr, payload)
		if err != nil {
			return bytesSent
		}

		// LK-TODO - check if we should keep separate padding stats
		size = hdr.MarshalSize() + len(payload)
		d.UpdateStats(uint32(size))

		// LK-TODO-START
		// NACK buffer for these probe packets.
		// Probably okay to absorb the NACKs for these and ignore them.
		// Retransmssion is probably a sign of network congestion/badness.
		// So, retransmitting padding packets is only going to make matters worse.
		// LK-TODO-END

		bytesSent += size
		bytesToSend -= size
		if bytesToSend <= 0 {
			break
		}
	}

	return bytesSent
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
		d.lossFraction.set(0)
		d.reSync.set(val)
	}

	if d.onSubscriptionChanged != nil {
		d.onSubscriptionChanged(d)
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

func (d *DownTrack) MaxSpatialLayer() int32 {
	return d.maxSpatialLayer.get()
}

// SwitchSpatialLayer switches the current layer
func (d *DownTrack) SwitchSpatialLayer(targetLayer int32, setAsMax bool) error {
	// LK-TODO-START
	// This gets called directly from two places outside.
	//   1. From livekit-server when suscriber updates TrackSetting
	//   2. From sfu.Receiver when a new up track is added
	// Make a method in this struct which allows setting subscriber controls.
	// StreamAllocator needs to know about subscriber setting changes.
	// Only the StreamAllocator should be changing forwarded layers.
	// Make a method `SetMaxSpatialLayer()` and remove `setAsMax` from this.
	// Trigger callback to StreamAllocator to notify subscription change
	// in that method.
	// LK-TODO-END
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

		if d.onSubscriptionChanged != nil {
			d.onSubscriptionChanged(d)
		}
	}
	return nil
}

func (d *DownTrack) SwitchSpatialLayerDone(layer int32) {
	d.currentSpatialLayer.set(layer)
}

func (d *DownTrack) UptrackLayersChange(availableLayers []uint16, layerAdded bool) (int32, error) {
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
			// LK-TODO-START
			// This layer switch should be removed when StreamAllocator is used.
			// Available layers change should be signalled to StreamAllocator
			// and StreamAllocator will take care of adjusting allocations.
			// LK-TODO-END
			if err := d.SwitchSpatialLayer(int32(targetLayer), false); err != nil {
				return int32(targetLayer), err
			}
		}

		if d.onAvailableLayersChanged != nil {
			d.onAvailableLayersChanged(d, layerAdded)
		}

		return int32(targetLayer), nil
	}
	return -1, fmt.Errorf("downtrack %s does not support simulcast", d.id)
}

func (d *DownTrack) SwitchTemporalLayer(targetLayer int32, setAsMax bool) {
	// LK-TODO-START
	// See note in SwitchSpatialLayer to split out setting max layer
	// into a separate method and triggering notification to StreamAllocator.
	// BTW, looks like `setAsMax` is not set to true at any callsite of this API
	// LK-TODO-END
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

			if d.onSubscriptionChanged != nil {
				d.onSubscriptionChanged(d)
			}
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

func (d *DownTrack) OnREMB(fn func(dt *DownTrack, remb *rtcp.ReceiverEstimatedMaximumBitrate)) {
	d.onREMB = fn
}

func (d *DownTrack) CurrentMaxLossFraction() uint8 {
	return d.lossFraction.get()
}

func (d *DownTrack) AddReceiverReportListener(listener ReceiverReportListener) {
	d.listenerLock.Lock()
	defer d.listenerLock.Unlock()
	d.receiverReportListeners = append(d.receiverReportListeners, listener)
}

func (d *DownTrack) OnAvailableLayersChanged(fn func(dt *DownTrack, layerAdded bool)) {
	d.onAvailableLayersChanged = fn
}

func (d *DownTrack) OnSubscriptionChanged(fn func(dt *DownTrack)) {
	d.onSubscriptionChanged = fn
}

func (d *DownTrack) OnPacketSent(fn func(dt *DownTrack, size int)) {
	d.onPacketSent = fn
}

func (d *DownTrack) AdjustAllocation(availableChannelCapacity uint64) (uint64, uint64) {
	if d.Kind() == webrtc.RTPCodecTypeAudio || !d.enabled.get() {
		return 0, 0
	}

	// LK-TODO for temporal preference, traverse the bitrates array the other way
	optimalBandwidthNeeded := uint64(0)
	brs := d.receiver.GetBitrateTemporalCumulative()
	for i := d.maxSpatialLayer.get(); i >= 0; i-- {
		for j := d.maxTemporalLayer.get(); j >= 0; j-- {
			if brs[i][j] == 0 {
				continue
			}
			if optimalBandwidthNeeded == 0 {
				optimalBandwidthNeeded = brs[i][j]
			}
			if brs[i][j] < availableChannelCapacity {
				d.bandwidthConstrainedMute(false) // just in case it was muted
				d.SwitchSpatialLayer(int32(i), false)
				d.SwitchTemporalLayer(int32(j), false)

				return brs[i][j], optimalBandwidthNeeded
			}
		}
	}

	// no layer fits in the available channel capacity, disable the track
	// LK-TODO - this should fire some callback to notify an observer that a
	// track has been disabled/muted due to bandwidth constraints
	d.bandwidthConstrainedMute(true)

	return 0, optimalBandwidthNeeded
}

func (d *DownTrack) IncreaseAllocation() (bool, uint64, uint64) {
	// LK-TODO-START
	// This is mainly used in probing to try a slightly higher layer.
	// But, if down track is not a simulcast track, then the next
	// available layer (i. e. the only layer of simple track) may boost
	// things by a lot (it could happen in simulcast jumps too).
	// May need to take in a layer increase threshold as an argument
	// (in terms of bps) and increase layer only if the jump is within
	// that threshold.
	// LK-TODO-END
	if d.Kind() == webrtc.RTPCodecTypeAudio || !d.enabled.get() {
		return false, 0, 0
	}

	currentSpatialLayer := d.CurrentSpatialLayer()
	targetSpatialLayer := d.TargetSpatialLayer()

	temporalLayer := d.temporalLayer.get()
	currentTemporalLayer := temporalLayer & 0x0f
	targetTemporalLayer := temporalLayer >> 16

	// if targets are still pending, don't increase
	if targetSpatialLayer != currentSpatialLayer || targetTemporalLayer != currentTemporalLayer {
		return false, 0, 0
	}

	// move to the next available layer
	optimalBandwidthNeeded := uint64(0)
	brs := d.receiver.GetBitrateTemporalCumulative()
	for i := d.maxSpatialLayer.get(); i >= 0; i-- {
		for j := d.maxTemporalLayer.get(); j >= 0; j-- {
			if brs[i][j] == 0 {
				continue
			}
			if optimalBandwidthNeeded == 0 {
				optimalBandwidthNeeded = brs[i][j]
				break
			}
		}

		if optimalBandwidthNeeded != 0 {
			break
		}
	}

	if d.bandwidthConstrainedMuted.get() {
		// try the lowest spatial and temporal layer if available
		// LK-TODO-START
		// note that this will never be zero because we do not track
		// layer 0 in available layers. So, this will need fixing.
		// LK-TODO-END
		if brs[0][0] == 0 {
			// no feed available
			return false, 0, 0
		}

		d.bandwidthConstrainedMute(false)
		d.SwitchSpatialLayer(int32(0), false)
		d.SwitchTemporalLayer(int32(0), false)
		return true, brs[0][0], optimalBandwidthNeeded
	}

	// try moving temporal layer up in the current spatial layer
	// LK-TODO currentTemporalLayer may be outside available range because of inital value being out of range, fix it
	nextTemporalLayer := currentTemporalLayer + 1
	if nextTemporalLayer <= d.maxTemporalLayer.get() && brs[currentSpatialLayer][nextTemporalLayer] > 0 {
		d.SwitchTemporalLayer(nextTemporalLayer, false)
		return true, brs[currentSpatialLayer][nextTemporalLayer], optimalBandwidthNeeded
	}

	// try moving spatial layer up if already at max temporal layer of current spatial layer
	// LK-TODO currentTemporalLayer may be outside available range because of inital value being out of range, fix it
	nextSpatialLayer := currentSpatialLayer + 1
	if nextSpatialLayer <= d.maxSpatialLayer.get() && brs[nextSpatialLayer][0] > 0 {
		d.SwitchSpatialLayer(nextSpatialLayer, false)
		d.SwitchTemporalLayer(0, false)
		return true, brs[nextSpatialLayer][0], optimalBandwidthNeeded
	}

	return false, 0, 0
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
	octets, packets := d.getSRStats()

	return &rtcp.SenderReport{
		SSRC:        d.ssrc,
		NTPTime:     uint64(nowNTP),
		RTPTime:     srRTP + uint32(diff),
		PacketCount: packets,
		OctetCount:  octets,
	}
}

func (d *DownTrack) UpdateStats(packetLen uint32) {
	d.octetCount.add(packetLen)
	d.packetCount.add(1)
}

// bandwidthConstrainedMute enables or disables media forwarding dictated by channel bandwidth constraints
func (d *DownTrack) bandwidthConstrainedMute(val bool) {
	if d.bandwidthConstrainedMuted.get() == val {
		return
	}
	d.bandwidthConstrainedMuted.set(val)
	if val {
		d.reSync.set(val)
	}
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
		if d.packetCount.get() > 0 {
			// LK-TODO-START
			// TS offset of 1 is not accurate. It should ideally
			// be driven by packetization of the incoming track.
			// But, this handles track switch on a simple track scenario.
			// It is not a supported use case. So, it is okay. But, if
			// we support switch track (i. e. same down track switches
			// to a different up track), this needs to be looked at.
			// LK-TODO-END
			d.munger.updateSnTsOffsets(extPkt, 1, 1)
		}
		d.lastSSRC.set(extPkt.Packet.SSRC)
		d.reSync.set(false)
	}

	// LK-TODO maybe include RTP header size also
	d.UpdateStats(uint32(len(extPkt.Packet.Payload)))

	newSN, newTS := d.munger.updateAndGetSnTs(extPkt)
	if d.sequencer != nil {
		d.sequencer.push(extPkt.Packet.SequenceNumber, newSN, newTS, 0, extPkt.Head)
	}

	hdr := extPkt.Packet.Header
	hdr.PayloadType = d.payloadType
	hdr.Timestamp = newTS
	hdr.SequenceNumber = newSN
	hdr.SSRC = d.ssrc

	err := d.WriteRTPHeaderExtensions(&hdr)
	if err != nil {
		return err
	}

	_, err = d.writeStream.WriteRTP(&hdr, extPkt.Packet.Payload)
	if err == nil && d.onPacketSent != nil {
		d.onPacketSent(d, hdr.MarshalSize()+len(extPkt.Packet.Payload))
	}

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

	// LK-TODO-START
	// The below offset calculation is not technically correct.
	// Timestamps based on the system time of an intermediate box like
	// SFU is not going to be accurate. Packets arrival/procesing
	// are subject vagaries of network delays, SFU processing etc.
	// But, the correct way is a lot harder. Will have to
	// look at RTCP SR to get timestamps and figure out alignment
	// of layers and use that during layer switch. That can
	// get tricky. Given the complexity of that approach, maybe
	// this is just fine till it is not :-).
	// LK-TODO-END

	// Compute how much time passed between the old RTP extPkt
	// and the current packet, and fix timestamp on source change
	lTSCalc := d.simulcast.lTSCalc.get()
	if lTSCalc != 0 && lastSSRC != extPkt.Packet.SSRC {
		d.lastSSRC.set(extPkt.Packet.SSRC)
		tDiff := (extPkt.Arrival - lTSCalc) / 1e6
		// LK-TODO-START
		// this is assuming clock rate of 90000.
		// Should be fine for video, but ideally should use ClockRate of the track
		// LK-TODO-END
		td := uint32((tDiff * 90) / 1000)
		if td == 0 {
			td = 1
		}
		d.munger.updateSnTsOffsets(extPkt, 1, td)
	} else if lTSCalc == 0 {
		d.munger.setLastSnTs(extPkt)
		if d.mime == "video/vp8" {
			if vp8, ok := extPkt.Payload.(buffer.VP8); ok {
				d.simulcast.temporalSupported = vp8.TemporalSupported
			}
		}
	}

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
				d.munger.addSnOffset(1)
				return nil
			}
		}
	}

	newSN, newTS := d.munger.updateAndGetSnTs(extPkt)
	if d.sequencer != nil {
		if meta := d.sequencer.push(extPkt.Packet.SequenceNumber, newSN, newTS, uint8(csl), extPkt.Head); meta != nil &&
			d.simulcast.temporalSupported && d.mime == "video/vp8" {
			meta.setVP8PayloadMeta(tlz0Idx, picID)
		}
	}

	// LK-TODO - maybe include RTP header?
	d.UpdateStats(uint32(len(payload)))

	// Update base
	d.simulcast.lTSCalc.set(extPkt.Arrival)
	// Update extPkt headers
	hdr := extPkt.Packet.Header
	hdr.SequenceNumber = newSN
	hdr.Timestamp = newTS
	hdr.SSRC = d.ssrc
	hdr.PayloadType = d.payloadType

	err := d.WriteRTPHeaderExtensions(&hdr)
	if err != nil {
		return err
	}

	_, err = d.writeStream.WriteRTP(&hdr, payload)
	if err == nil && d.onPacketSent != nil {
		d.onPacketSent(d, hdr.MarshalSize()+len(payload))
	}

	return err
}

func (d *DownTrack) handleRTCP(bytes []byte) {
	// LK-TODO - should probably handle RTCP even if muted
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
			// LK-TODO-START
			// Remove expectedMinBitrate calculation when switching code
			// to StreamAllocator based layer control
			// LK-TODO-END
			if expectedMinBitrate == 0 || expectedMinBitrate > uint64(p.Bitrate) {
				expectedMinBitrate = uint64(p.Bitrate)
			}
			if d.onREMB != nil {
				d.onREMB(d, p)
			}
		case *rtcp.ReceiverReport:
			// create new receiver report w/ only valid reception reports
			rr := &rtcp.ReceiverReport{
				SSRC:              p.SSRC,
				ProfileExtensions: p.ProfileExtensions,
			}
			for _, r := range p.Reports {
				if r.SSRC != d.ssrc {
					continue
				}
				rr.Reports = append(rr.Reports, r)
				if maxRatePacketLoss == 0 || maxRatePacketLoss < r.FractionLost {
					maxRatePacketLoss = r.FractionLost
				}
			}
			d.lossFraction.set(maxRatePacketLoss)
			if len(rr.Reports) > 0 {
				d.listenerLock.RLock()
				for _, l := range d.receiverReportListeners {
					l(d, rr)
				}
				d.listenerLock.RUnlock()
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

	// LK-TODO: Remove when switching to StreamAllocator based layer control
	if d.trackType == SimulcastDownTrack && (maxRatePacketLoss != 0 || expectedMinBitrate != 0) {
		d.handleLayerChange(maxRatePacketLoss, expectedMinBitrate)
	}

	if len(fwdPkts) > 0 {
		d.receiver.SendRTCP(fwdPkts)
	}
}

// LK-TODO: Remove when switching to StreamAllocator based layer control
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

func (d *DownTrack) getSRStats() (octets, packets uint32) {
	return d.octetCount.get(), d.packetCount.get()
}

func (d *DownTrack) DebugInfo() map[string]interface{} {
	mungerParams := d.munger.getParams()
	stats := map[string]interface{}{
		"LastSN":         mungerParams.lastSN,
		"SNOffset":       mungerParams.snOffset,
		"LastTS":         mungerParams.lastTS,
		"TSOffset":       mungerParams.tsOffset,
		"LastMarker":     mungerParams.lastMarker,
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

//
// munger
//
type MungerParams struct {
	snOffset   uint16
	lastSN     uint16
	tsOffset   uint32
	lastTS     uint32
	lastMarker bool
}

type Munger struct {
	lock sync.RWMutex

	MungerParams
}

func (m *Munger) getParams() MungerParams {
	m.lock.RLock()
	defer m.lock.RUnlock()

	return MungerParams{
		snOffset:   m.snOffset,
		lastSN:     m.lastSN,
		tsOffset:   m.tsOffset,
		lastTS:     m.lastTS,
		lastMarker: m.lastMarker,
	}
}

func (m *Munger) setLastSnTs(extPkt *buffer.ExtPacket) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.lastSN = extPkt.Packet.SequenceNumber
	m.lastTS = extPkt.Packet.Timestamp
}

func (m *Munger) updateSnTsOffsets(extPkt *buffer.ExtPacket, snAdjust uint16, tsAdjust uint32) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.snOffset = extPkt.Packet.SequenceNumber - m.lastSN - snAdjust
	m.tsOffset = extPkt.Packet.Timestamp - m.lastTS - tsAdjust
}

func (m *Munger) addSnOffset(snAdjust uint16) {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.snOffset += snAdjust
}

func (m *Munger) updateAndGetSnTs(extPkt *buffer.ExtPacket) (uint16, uint32) {
	m.lock.Lock()
	defer m.lock.Unlock()

	mungedSN := extPkt.Packet.SequenceNumber - m.snOffset
	mungedTS := extPkt.Packet.Timestamp - m.tsOffset

	if extPkt.Head {
		m.lastSN = mungedSN
		m.lastTS = mungedTS
		m.lastMarker = extPkt.Packet.Marker
	}

	return mungedSN, mungedTS
}

func (m *Munger) updateAndGetPaddingSnTs() (uint16, uint32, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if !m.lastMarker {
		return 0, 0, ErrPaddingNotOnFrameBoundary
	}

	sn := m.lastSN + 1
	ts := m.lastTS

	m.lastSN = sn
	m.snOffset -= 1

	return sn, ts, nil
}
