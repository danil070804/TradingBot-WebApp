from dataclasses import dataclass


@dataclass(frozen=True)
class IdentityResolutionResult:
    user_id: int | None
    error: str | None = None
    status_code: int = 200


def resolve_session_user_id(
    session_user_id: int | None,
    fallback_user_id: int | None,
    payload_user_id: int | None,
    *,
    require_session: bool = True,
) -> IdentityResolutionResult:
    """Return a trusted user id for API calls and reject identity spoofing."""
    if require_session and not session_user_id:
        return IdentityResolutionResult(
            user_id=None,
            error="Требуется авторизация через Telegram WebApp",
            status_code=401,
        )

    current_user_id = int(session_user_id) if session_user_id else int(fallback_user_id or 0)
    if not current_user_id:
        return IdentityResolutionResult(
            user_id=None,
            error="Пользователь не определён",
            status_code=401,
        )

    if payload_user_id is not None and int(payload_user_id) != current_user_id:
        return IdentityResolutionResult(
            user_id=None,
            error="Несовпадение пользователя с текущей сессией",
            status_code=403,
        )

    return IdentityResolutionResult(user_id=current_user_id)
