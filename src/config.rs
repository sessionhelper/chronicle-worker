/// Worker configuration from environment variables.
pub struct Config {
    pub data_api_url: String,
    pub shared_secret: String,
    pub whisper_url: String,
    pub whisper_model: String,
    pub vad_model_path: String,
    pub poll_interval_secs: u64,
    /// LLM endpoint for scene detection (optional).
    pub scene_llm_url: Option<String>,
    pub scene_llm_model: String,
    /// GM speaker pseudo_id for scene detection.
    pub gm_speaker_id: Option<String>,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            data_api_url: std::env::var("DATA_API_URL")
                .unwrap_or_else(|_| "http://127.0.0.1:8001".into()),
            shared_secret: std::env::var("DATA_API_SHARED_SECRET")
                .expect("DATA_API_SHARED_SECRET must be set"),
            whisper_url: std::env::var("WHISPER_URL")
                .unwrap_or_else(|_| "http://localhost:8300/v1/audio/transcriptions".into()),
            whisper_model: std::env::var("WHISPER_MODEL")
                .unwrap_or_else(|_| "deepdml/faster-whisper-large-v3-turbo-ct2".into()),
            vad_model_path: std::env::var("VAD_MODEL_PATH")
                .unwrap_or_else(|_| "models/silero_vad_v6.onnx".into()),
            poll_interval_secs: std::env::var("POLL_INTERVAL_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(10),
            scene_llm_url: std::env::var("SCENE_LLM_URL").ok(),
            scene_llm_model: std::env::var("SCENE_LLM_MODEL")
                .unwrap_or_else(|_| "qwen2.5:7b".into()),
            gm_speaker_id: std::env::var("GM_SPEAKER_ID").ok(),
        }
    }
}
