"""Generic LLM routing utilities for utility/reasoning/web roles."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any


OPENAI_CHAT_COMPLETIONS_ENDPOINT = "https://api.openai.com/v1/chat/completions"
OPENROUTER_CHAT_COMPLETIONS_ENDPOINT = "https://openrouter.ai/api/v1/chat/completions"
GOOGLE_GENERATE_CONTENT_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={key}"
SUPPORTED_PROVIDERS = {"google", "openrouter", "openai"}
DEFAULT_MODELS = {
    "google": "gemini-2.5-flash-lite",
    "openrouter": "deepseek/deepseek-r1",
    "openai": "gpt-4o-mini",
}


def _env_flag(key: str, default: bool) -> bool:
    value = os.getenv(key, "").strip().lower()
    if not value:
        return default
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def llm_enabled() -> bool:
    explicit = os.getenv("LLM_ENABLED", "").strip()
    if explicit:
        return _env_flag("LLM_ENABLED", default=True)
    # Backward compatibility with old flag name.
    legacy = os.getenv("INTERPRET_WITH_LLM", "").strip()
    if legacy:
        return _env_flag("INTERPRET_WITH_LLM", default=True)
    return any(provider_key(provider) for provider in SUPPORTED_PROVIDERS)


def provider_key(provider: str) -> str:
    normalized = provider.strip().lower()
    if normalized == "google":
        return os.getenv("GOOGLE_API_KEY", "").strip()
    if normalized == "openrouter":
        return os.getenv("OPENROUTER_API_KEY", "").strip()
    if normalized == "openai":
        return os.getenv("OPENAI_API_KEY", "").strip()
    return ""


def provider_available(provider: str) -> bool:
    return bool(provider_key(provider))


def configured_providers() -> list[str]:
    return [provider for provider in ("google", "openrouter", "openai") if provider_available(provider)]


def _provider_candidates(role: str) -> list[str]:
    role_key = role.strip().upper() or "UTILITY"
    role_provider = os.getenv(f"LLM_{role_key}_PROVIDER", "").strip().lower()
    base_provider = os.getenv("LLM_PROVIDER", "").strip().lower()
    fallback = os.getenv("LLM_FALLBACK_PROVIDERS", "").strip().lower()
    fallback_items = [item.strip() for item in fallback.split(",") if item.strip()]
    candidates: list[str] = []
    for provider in [role_provider, base_provider, *fallback_items]:
        if provider and provider in SUPPORTED_PROVIDERS and provider not in candidates:
            candidates.append(provider)
    for provider in ("google", "openrouter", "openai"):
        if provider not in candidates:
            candidates.append(provider)
    return candidates


def resolve_provider(role: str = "utility") -> str | None:
    for provider in _provider_candidates(role):
        if provider_available(provider):
            return provider
    return None


def _model_for_provider(provider: str, role: str) -> str:
    role_key = role.strip().upper() or "UTILITY"
    role_model = os.getenv(f"LLM_{role_key}_MODEL", "").strip()
    if role_model:
        return role_model
    base_model = os.getenv("LLM_MODEL", "").strip()
    if base_model:
        return base_model
    if role.lower() == "utility":
        # Backward compatibility with existing env usage in the repo.
        legacy_model = os.getenv("PRA_FAST_MODEL", "").strip()
        if legacy_model:
            return legacy_model
    return DEFAULT_MODELS.get(provider, "gpt-4o-mini")


def _chat_completions_request(
    endpoint: str,
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    timeout_seconds: int,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    payload = {
        "model": model,
        "temperature": temperature,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    }
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }
    if extra_headers:
        headers.update(extra_headers)
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _google_generate_content_request(
    api_key: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float,
    timeout_seconds: int,
) -> dict[str, Any]:
    endpoint = GOOGLE_GENERATE_CONTENT_ENDPOINT.format(model=model, key=api_key)
    payload = {
        "systemInstruction": {
            "parts": [{"text": system_prompt}],
        },
        "contents": [
            {
                "role": "user",
                "parts": [{"text": user_prompt}],
            }
        ],
        "generationConfig": {
            "temperature": temperature,
            "responseMimeType": "application/json",
        },
    }
    request = urllib.request.Request(
        endpoint,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _extract_content(provider: str, body: dict[str, Any]) -> str:
    if provider in {"openai", "openrouter"}:
        choices = body.get("choices")
        if isinstance(choices, list) and choices:
            message = choices[0].get("message", {})
            return str(message.get("content") or "").strip()
        return ""
    if provider == "google":
        candidates = body.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            return ""
        content = candidates[0].get("content", {})
        parts = content.get("parts", [])
        if not isinstance(parts, list):
            return ""
        joined = "".join(str(part.get("text") or "") for part in parts if isinstance(part, dict))
        return joined.strip()
    return ""


def call_llm(
    *,
    role: str,
    system_prompt: str,
    user_prompt: str,
    temperature: float = 0.2,
    timeout_seconds: int = 18,
) -> dict[str, Any]:
    """Call the best available provider and return a normalized response payload."""
    if not llm_enabled():
        return {"ok": False, "error": "llm_disabled"}

    provider = resolve_provider(role=role)
    if not provider:
        return {"ok": False, "error": "no_provider_configured"}
    api_key = provider_key(provider)
    if not api_key:
        return {"ok": False, "error": f"missing_api_key:{provider}"}

    model = _model_for_provider(provider, role=role)
    try:
        if provider == "google":
            body = _google_generate_content_request(
                api_key=api_key,
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                timeout_seconds=timeout_seconds,
            )
        elif provider == "openrouter":
            body = _chat_completions_request(
                endpoint=OPENROUTER_CHAT_COMPLETIONS_ENDPOINT,
                api_key=api_key,
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                timeout_seconds=timeout_seconds,
                extra_headers={
                    "HTTP-Referer": "https://local.personal-research-agent-v3",
                    "X-Title": "Personal Research Agent v3",
                },
            )
        else:
            body = _chat_completions_request(
                endpoint=OPENAI_CHAT_COMPLETIONS_ENDPOINT,
                api_key=api_key,
                model=model,
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                temperature=temperature,
                timeout_seconds=timeout_seconds,
            )
        content = _extract_content(provider, body)
        if not content:
            return {"ok": False, "error": f"empty_response:{provider}", "provider": provider, "model": model}
        return {"ok": True, "provider": provider, "model": model, "content": content}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")[:240]
        return {
            "ok": False,
            "error": f"http_error:{provider}:{exc.code}",
            "provider": provider,
            "model": model,
            "body": body,
        }
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError) as exc:
        return {"ok": False, "error": f"request_error:{provider}:{exc.__class__.__name__}", "provider": provider, "model": model}

