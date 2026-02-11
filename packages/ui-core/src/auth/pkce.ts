/**
 * PKCE (Proof Key for Code Exchange) utilities.
 * Generates code_verifier and code_challenge for OAuth2 PKCE flow.
 */

/**
 * Generate a cryptographically random code_verifier string.
 * Per RFC 7636, must be 43-128 characters from [A-Z, a-z, 0-9, -, ., _, ~].
 */
export function generateCodeVerifier(length: number = 64): string {
  const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
  const values = new Uint8Array(length);
  crypto.getRandomValues(values);
  return Array.from(values, (v) => charset[v % charset.length]).join("");
}

/**
 * Generate a code_challenge from a code_verifier using SHA-256 + base64url.
 */
export async function generateCodeChallenge(verifier: string): Promise<string> {
  const encoder = new TextEncoder();
  const data = encoder.encode(verifier);
  const digest = await crypto.subtle.digest("SHA-256", data);
  return base64UrlEncode(new Uint8Array(digest));
}

/**
 * Base64url encode (no padding, URL-safe characters).
 */
function base64UrlEncode(bytes: Uint8Array): string {
  const binString = Array.from(bytes, (b) => String.fromCharCode(b)).join("");
  return btoa(binString)
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/, "");
}
