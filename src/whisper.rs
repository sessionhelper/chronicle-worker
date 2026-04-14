//! HTTP `WhisperClient` implementation the pipeline talks to.
//!
//! `chronicle-pipeline` owns the [`WhisperClient`] trait; this module
//! supplies the production implementation that POSTs to `WHISPER_URL`.
//! The pipeline layers its own exponential-backoff retry (3x from 500ms)
//! on top, so we only classify failures as transient vs. fatal here.
//!
//! Audio arrives as mono 16 kHz f32 samples (pipeline's VAD resample
//! target). We encode to a WAV buffer and send it as multipart form data,
//! mirroring OpenAI's audio/transcriptions contract — faster-whisper,
//! whisperx, and the local faster-whisper HTTP shim all speak this.

use std::io::{Cursor, Write};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chronicle_pipeline::{Transcription, WhisperClient, WhisperError};
use reqwest::{multipart, Client};
use serde::Deserialize;

use crate::config::Config;

/// The pipeline expects 16 kHz mono f32 audio at the trait boundary.
const WAV_BYTES_PER_SAMPLE: u16 = 2;

/// Production Whisper HTTP client. Cheaply clonable via internal `Arc`s.
#[derive(Clone)]
pub struct HttpWhisperClient {
    http: Client,
    endpoint: String,
    model: String,
    language: Option<String>,
    initial_prompt: Option<String>,
    temperature: f32,
}

impl HttpWhisperClient {
    /// Build the production Whisper client from worker config.
    pub fn from_config(cfg: &Config) -> Result<Self, WhisperError> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|e| WhisperError::Fatal(format!("http build: {e}")))?;

        Ok(Self {
            http,
            endpoint: cfg.whisper_url.clone(),
            model: cfg.whisper_model.clone(),
            language: Some("en".into()),
            initial_prompt: cfg.whisper_initial_prompt.clone(),
            temperature: 0.0,
        })
    }
}

#[derive(Deserialize)]
struct WhisperResp {
    #[serde(default)]
    text: String,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    avg_logprob: Option<f32>,
    /// Some backends return per-segment logprobs; we average if `avg_logprob`
    /// is missing.
    #[serde(default)]
    segments: Vec<WhisperSegmentSnippet>,
}

#[derive(Deserialize)]
struct WhisperSegmentSnippet {
    #[serde(default)]
    avg_logprob: Option<f32>,
}

#[async_trait]
impl WhisperClient for HttpWhisperClient {
    async fn transcribe(
        &self,
        audio: &[f32],
        sample_rate: u32,
    ) -> Result<Transcription, WhisperError> {
        if audio.is_empty() {
            return Err(WhisperError::Fatal("empty audio".into()));
        }

        let wav = encode_wav_pcm16(audio, sample_rate)
            .map_err(|e| WhisperError::Fatal(format!("wav encode: {e}")))?;

        let mut form = multipart::Form::new()
            .text("model", self.model.clone())
            .text("response_format", "json")
            .text("temperature", self.temperature.to_string())
            .part(
                "file",
                multipart::Part::bytes(wav)
                    .file_name("audio.wav")
                    .mime_str("audio/wav")
                    .map_err(|e| WhisperError::Fatal(format!("mime: {e}")))?,
            );
        if let Some(lang) = &self.language {
            form = form.text("language", lang.clone());
        }
        if let Some(prompt) = &self.initial_prompt {
            form = form.text("prompt", prompt.clone());
        }

        let started = Instant::now();
        let resp = self
            .http
            .post(&self.endpoint)
            .multipart(form)
            .send()
            .await
            .map_err(|e| classify(&e))?;

        let status = resp.status();
        if !status.is_success() {
            let body = resp.text().await.unwrap_or_default();
            let msg = format!("{status}: {body}");
            // 5xx / 408 / 429 → transient; everything else → fatal.
            if status.is_server_error()
                || status == reqwest::StatusCode::REQUEST_TIMEOUT
                || status == reqwest::StatusCode::TOO_MANY_REQUESTS
            {
                return Err(WhisperError::Transient(msg));
            }
            return Err(WhisperError::Fatal(msg));
        }

        let parsed: WhisperResp = resp
            .json()
            .await
            .map_err(|e| WhisperError::Transient(format!("json: {e}")))?;

        let confidence = parsed
            .avg_logprob
            .or_else(|| average_segment_logprob(&parsed.segments))
            .unwrap_or(-0.2);

        tracing::debug!(
            elapsed_ms = started.elapsed().as_millis() as u64,
            chars = parsed.text.len(),
            "whisper transcription complete"
        );

        Ok(Transcription {
            text: parsed.text,
            confidence,
            language: parsed.language,
        })
    }
}

fn classify(e: &reqwest::Error) -> WhisperError {
    // Connect / timeout / io errors are transient; protocol errors are fatal.
    if e.is_timeout() || e.is_connect() || e.is_request() {
        WhisperError::Transient(e.to_string())
    } else {
        WhisperError::Fatal(e.to_string())
    }
}

fn average_segment_logprob(segments: &[WhisperSegmentSnippet]) -> Option<f32> {
    let vals: Vec<f32> = segments.iter().filter_map(|s| s.avg_logprob).collect();
    if vals.is_empty() {
        return None;
    }
    Some(vals.iter().sum::<f32>() / vals.len() as f32)
}

/// Encode mono f32 samples at `sample_rate` as a 16-bit PCM WAV buffer.
fn encode_wav_pcm16(audio: &[f32], sample_rate: u32) -> std::io::Result<Vec<u8>> {
    let num_samples = audio.len() as u32;
    let data_len = num_samples * WAV_BYTES_PER_SAMPLE as u32;
    let chunk_size = 36 + data_len;

    let mut buf = Cursor::new(Vec::with_capacity(44 + data_len as usize));
    buf.write_all(b"RIFF")?;
    buf.write_all(&chunk_size.to_le_bytes())?;
    buf.write_all(b"WAVE")?;
    buf.write_all(b"fmt ")?;
    buf.write_all(&16u32.to_le_bytes())?; // PCM subchunk size
    buf.write_all(&1u16.to_le_bytes())?; // PCM format
    buf.write_all(&1u16.to_le_bytes())?; // channels (mono)
    buf.write_all(&sample_rate.to_le_bytes())?;
    let byte_rate = sample_rate * WAV_BYTES_PER_SAMPLE as u32;
    buf.write_all(&byte_rate.to_le_bytes())?;
    buf.write_all(&WAV_BYTES_PER_SAMPLE.to_le_bytes())?; // block align
    buf.write_all(&16u16.to_le_bytes())?; // bits per sample
    buf.write_all(b"data")?;
    buf.write_all(&data_len.to_le_bytes())?;
    for s in audio {
        let clamped = s.clamp(-1.0, 1.0);
        let v = (clamped * i16::MAX as f32) as i16;
        buf.write_all(&v.to_le_bytes())?;
    }
    Ok(buf.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wav_header_is_44_bytes_for_nonzero_audio() {
        let wav = encode_wav_pcm16(&[0.0; 8], 16_000).unwrap();
        assert_eq!(&wav[..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        // 44-byte header + 8 samples * 2 bytes = 60
        assert_eq!(wav.len(), 44 + 16);
    }
}
