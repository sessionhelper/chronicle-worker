//! Raw PCM decoding helpers.
//!
//! The collector uploads audio as signed 16-bit little-endian stereo PCM
//! at Discord's native 48kHz rate. `ovp-pipeline` wants mono f32 samples
//! in `[-1.0, 1.0]`. This module owns the conversion and nothing else so
//! it can be exercised in isolation by unit tests.
//!
//! Kept in a dedicated module (instead of inlined in `worker.rs`) for
//! the same reason every codec lives in its own file: the edge cases
//! (odd byte counts, odd sample counts, saturation) are easier to reason
//! about when they're not tangled with orchestration code.

/// Decode interleaved s16le stereo bytes into mono f32 samples.
///
/// Two channels are averaged per frame. Trailing bytes that don't form a
/// complete stereo frame (4 bytes: L-lo, L-hi, R-lo, R-hi) are silently
/// dropped — the function never panics on ragged input. This matters
/// because chunk boundaries can split mid-frame when the collector flushes
/// a buffer, and the concatenated stream only holds the whole-frame
/// invariant *across* the session, not necessarily at the very end.
///
/// Normalisation is `i16::MAX` (32767), not `32768`, so a sample of
/// `i16::MIN` (-32768) maps to ~-1.00003. Clamping would cost a branch
/// per sample for a quarter-LSB of headroom we don't need; downstream
/// Whisper tolerates values slightly outside `[-1, 1]` just fine.
pub fn decode_stereo_to_mono(raw: &[u8]) -> Vec<f32> {
    // 4 bytes per stereo frame (2 bytes/sample * 2 channels).
    let frames = raw.len() / 4;
    let mut out = Vec::with_capacity(frames);
    for frame in raw.chunks_exact(4) {
        let l = i16::from_le_bytes([frame[0], frame[1]]) as f32;
        let r = i16::from_le_bytes([frame[2], frame[3]]) as f32;
        // Average first, then scale. Avoids overflow worries since both
        // operands fit in f32 losslessly.
        out.push(((l + r) * 0.5) / i16::MAX as f32);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_produces_empty_output() {
        assert!(decode_stereo_to_mono(&[]).is_empty());
    }

    #[test]
    fn single_stereo_frame_averages_channels() {
        // L = 16384 (0x4000), R = -16384 (0xC000). Average = 0.
        let raw = [0x00, 0x40, 0x00, 0xC0];
        let out = decode_stereo_to_mono(&raw);
        assert_eq!(out.len(), 1);
        assert!(out[0].abs() < 1e-6, "expected ~0, got {}", out[0]);
    }

    #[test]
    fn full_scale_positive_frame_near_one() {
        // Both channels at i16::MAX -> mono sample = 1.0 exactly.
        let hi = i16::MAX.to_le_bytes();
        let raw = [hi[0], hi[1], hi[0], hi[1]];
        let out = decode_stereo_to_mono(&raw);
        assert_eq!(out.len(), 1);
        assert!((out[0] - 1.0).abs() < 1e-6);
    }

    #[test]
    fn many_frames_round_trip_each_sample() {
        // Build a deterministic ramp across 1000 frames and verify the
        // decoded output matches the expected average.
        let mut raw = Vec::with_capacity(4000);
        for i in 0..1000i16 {
            raw.extend_from_slice(&i.to_le_bytes());
            raw.extend_from_slice(&(-i).to_le_bytes());
        }
        let out = decode_stereo_to_mono(&raw);
        assert_eq!(out.len(), 1000);
        // L + (-L) = 0 for every frame.
        for (idx, s) in out.iter().enumerate() {
            assert!(s.abs() < 1e-6, "frame {idx} not ~0: {s}");
        }
    }

    #[test]
    fn odd_length_input_truncates_trailing_partial_frame() {
        // 5 bytes: one whole frame (4) + one stray byte. The stray byte
        // must be silently dropped, not panic or yield a garbage sample.
        let raw = [0x00, 0x40, 0x00, 0xC0, 0xFF];
        let out = decode_stereo_to_mono(&raw);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn three_bytes_yields_nothing() {
        let out = decode_stereo_to_mono(&[0x01, 0x02, 0x03]);
        assert!(out.is_empty());
    }

    #[test]
    fn decoded_samples_are_finite() {
        let raw: Vec<u8> = (0..4000).map(|i| (i % 256) as u8).collect();
        let out = decode_stereo_to_mono(&raw);
        for s in &out {
            assert!(s.is_finite(), "non-finite sample: {s}");
        }
    }
}
