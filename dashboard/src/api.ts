/**
 * Bearer-authenticated client for the engine API (R10).
 *
 * The page is served BY the engine, so the API base is `window.location.origin`. `mt_api_base` in
 * localStorage overrides it for `npm run dev` (the Vite dev server is on another port). The token
 * lives in localStorage under `mt_token` and goes on EVERY request as `Authorization: Bearer` — the
 * WebSocket handshake cannot carry a header, so it uses the `?token=` query the engine also accepts.
 */

const TOKEN_KEY = 'mt_token'
const API_BASE_KEY = 'mt_api_base'

export function getToken(): string {
  return localStorage.getItem(TOKEN_KEY) ?? ''
}

export function setToken(token: string): void {
  if (token) localStorage.setItem(TOKEN_KEY, token)
  else localStorage.removeItem(TOKEN_KEY)
}

export function apiBase(): string {
  return (localStorage.getItem(API_BASE_KEY) ?? window.location.origin).replace(/\/$/, '')
}

/** Thrown for any non-2xx response; `status` lets a caller distinguish 401 (bad token) from the rest. */
export class ApiError extends Error {
  readonly status: number

  constructor(status: number, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  headers.set('Authorization', `Bearer ${getToken()}`)
  if (init.body !== undefined) headers.set('Content-Type', 'application/json')

  const res = await fetch(`${apiBase()}${path}`, { ...init, headers })
  if (!res.ok) {
    // FastAPI puts the reason in `detail`; the two-step/stub refusals answer 409/501 with `note`.
    let detail = res.statusText
    try {
      const body = (await res.json()) as { detail?: unknown; note?: unknown }
      detail = String(body.detail ?? body.note ?? detail)
    } catch {
      /* non-JSON body — keep the status text */
    }
    throw new ApiError(res.status, detail)
  }
  return (await res.json()) as T
}

export function apiGet<T>(path: string): Promise<T> {
  return request<T>(path, { method: 'GET' })
}

export function apiPost<T>(path: string, body: unknown): Promise<T> {
  return request<T>(path, { method: 'POST', body: JSON.stringify(body) })
}

/** `/ws/live` URL with the bearer token as a query param (R10: header auth is impossible on a WS
 *  handshake; the engine's `_ws_authorized` accepts either). */
export function liveSocketUrl(): string {
  const base = apiBase().replace(/^http/, 'ws')
  return `${base}/ws/live?token=${encodeURIComponent(getToken())}`
}
