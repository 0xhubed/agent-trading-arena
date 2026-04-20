"""Shared model registry for OpenAI-compatible LLM endpoints.

All LLM-based traders import from here to avoid duplicate model dicts.
Supports shorthand names (e.g. "llama-4-scout") or pass-through full model paths.
"""

from __future__ import annotations

# Model shorthand -> full OpenAI-compatible model path
LLM_MODELS: dict[str, str] = {
    # GPT-OSS models
    "gpt-oss-20b": "openai/gpt-oss-20b",
    "gpt-oss-120b": "openai/gpt-oss-120b",
    # GLM models
    "glm-5": "zai-org/GLM-5",
    "glm-4.5-air": "THUDM/GLM-4.5-Air",
    # Llama models
    "llama-4-scout": "meta-llama/Llama-4-Scout-17B-16E-Instruct",
    "llama-4-maverick": "meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8",
    "llama-3.3-70b": "meta-llama/Llama-3.3-70B-Instruct-Turbo",
    # Qwen models
    "qwen3.5-397b": "qwen3.5:397b-cloud",                              # Ollama Cloud
    "qwen3.5-122b": "qwen/qwen3.5-122b-a10b",                       # OpenRouter
    "qwen3-235b": "Qwen/Qwen3-235B-A22B-Instruct-2507-tput",
    "qwen-2.5-72b": "Qwen/Qwen2.5-72B-Instruct-Turbo",
    "qwen-2.5-7b": "Qwen/Qwen2.5-7B-Instruct-Turbo",
    # DeepSeek models
    "deepseek-v3": "deepseek-ai/DeepSeek-V3",
    "deepseek-v3.1": "deepseek-ai/DeepSeek-V3.1",
    "deepseek-r1": "deepseek-ai/DeepSeek-R1",
    # Legacy local inference pass-through (kept for local_inference.yaml)
    "nemotron": "nemotron",
    "glm-4.7-flash": "glm-4.7-flash",
}


def resolve_model(model_input: str) -> str:
    """Resolve a model shorthand to its full path, or pass through as-is."""
    return LLM_MODELS.get(model_input, model_input)
