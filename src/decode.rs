//! Raw PCM decoding helpers.
//!
//! The collector uploads audio as signed 16-bit little-endian stereo PCM
//! at Discord's native 48 kHz rate. `chronicle-pipeline`'s `AudioChunk`
//! wants mono **s16** at 48 kHz (the pipeline owns f32 conversion + VAD
//! resampling internally). This module owns the stereo→mono downmix and
//! nothing else so it can be exercised in isolation by unit tests.

/// Decode interleaved s16le stereo bytes into mono s16 samples.
///
/// Two channels are averaged per frame. Trailing bytes that don't form a
/// complete stereo frame (4 bytes: L-lo, L-hi, R-lo, R-hi) are silently
/// dropped — the function never panics on ragged input. Chunk boundaries
/// occasionally split mid-frame when the collector flushes a buffer; the
/// concatenated stream only holds the whole-frame invariant *across* the
/// session, not necessarily at the very end.
///
/// Averaging uses i32 intermediate to avoid i16 overflow when both
/// channels are near saturation.
pub fn decode_stereo_to_mono_i16(raw: &[u8]) -> Vec<i16> {
    let frames = raw.len() / 4;
    let mut out = Vec::with_capacity(frames);
    for frame in raw.chunks_exact(4) {
        let l = i16::from_le_bytes([frame[0], frame[1]]) as i32;
        let r = i16::from_le_bytes([frame[2], frame[3]]) as i32;
        out.push(((l + r) / 2) as i16);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_input_produces_empty_output() {
        assert!(decode_stereo_to_mono_i16(&[]).is_empty());
    }

    #[test]
    fn single_stereo_frame_averages_channels() {
        // L = 16384, R = -16384. Average = 0.
        let raw = [0x00, 0x40, 0x00, 0xC0];
        let out = decode_stereo_to_mono_i16(&raw);
        assert_eq!(out, vec![0]);
    }

    #[test]
    fn full_scale_positive_frame_near_peak() {
        let hi = i16::MAX.to_le_bytes();
        let raw = [hi[0], hi[1], hi[0], hi[1]];
        let out = decode_stereo_to_mono_i16(&raw);
        assert_eq!(out, vec![i16::MAX]);
    }

    #[test]
    fn odd_length_input_truncates_trailing_partial_frame() {
        let raw = [0x00, 0x40, 0x00, 0xC0, 0xFF];
        assert_eq!(decode_stereo_to_mono_i16(&raw).len(), 1);
    }

    #[test]
    fn three_bytes_yields_nothing() {
        assert!(decode_stereo_to_mono_i16(&[0x01, 0x02, 0x03]).is_empty());
    }

    #[test]
    fn no_i16_overflow_near_saturation() {
        // Both channels at i16::MIN would overflow naive i16 sum; ensure safe.
        let lo = i16::MIN.to_le_bytes();
        let raw = [lo[0], lo[1], lo[0], lo[1]];
        let out = decode_stereo_to_mono_i16(&raw);
        assert_eq!(out, vec![i16::MIN]);
    }
}
