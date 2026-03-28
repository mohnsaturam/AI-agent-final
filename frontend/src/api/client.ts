/**
 * UMSA API Client — Typed REST Client (Section 22)
 *
 * Consumes versioned REST endpoints from the FastAPI backend.
 * All requests include Bearer token authentication.
 */

// ============================================================
// Types
// ============================================================

export interface APIResponse<T = Record<string, unknown>> {
    status: string;
    data: T | null;
    error: { message: string;[key: string]: unknown } | null;
}

export interface ScrapeRequest {
    query: string;
    sites: string[];
    domain?: string;
    unify?: boolean;
}

export interface ScrapeResult {
    request_id: string;
    domain: string;
    query: string;
    intent: Record<string, unknown>;
    unified_data: Record<string, unknown>;
    sources: string[];
    confidence: number;
    resolved_conflicts: Record<string, unknown>;
    partial_failures: Array<{
        site_url: string;
        error: string;
        failure_class: string;
    }>;
}

export interface RequestStatus {
    request_id: string;
    state: string;
    domain: string;
    query: string;
    created_at: string;
    updated_at: string;
    result?: ScrapeResult;
}

export interface DomainInfo {
    name: string;
    schema_version: string;
    status: string;
}

export interface HealthStatus {
    database: string;
    active_domains: string[];
    semaphores: {
        global_pipeline: { max: number; available: number };
        global_playwright: { max: number; available: number };
        active_requests: number;
    };
    domain_health: Array<{
        domain: string;
        site: string;
        status: string;
        failures: number;
    }>;
}

// ============================================================
// API Client
// ============================================================

export class UMSAClient {
    private baseUrl: string;
    private token: string;

    constructor(baseUrl: string, token: string) {
        this.baseUrl = baseUrl.replace(/\/+$/, "");
        this.token = token;
    }

    /**
     * Set or update the Bearer token.
     */
    setToken(token: string): void {
        this.token = token;
    }

    /**
     * POST /v1/scrape — Submit a scrape request.
     */
    async submitScrape(
        request: ScrapeRequest
    ): Promise<APIResponse<{ request_id: string; state: string }>> {
        return this.request<{ request_id: string; state: string }>(
            "POST",
            "/v1/scrape",
            request
        );
    }

    /**
     * GET /v1/requests/:id — Poll request status and result.
     */
    async getRequestStatus(
        requestId: string
    ): Promise<APIResponse<RequestStatus>> {
        return this.request<RequestStatus>("GET", `/v1/requests/${requestId}`);
    }

    /**
     * GET /v1/domains — List active domains.
     */
    async listDomains(): Promise<APIResponse<{ domains: DomainInfo[] }>> {
        return this.request<{ domains: DomainInfo[] }>("GET", "/v1/domains");
    }

    /**
     * GET /v1/domains/:name/sites — List allowed sites for a domain.
     */
    async getDomainSites(
        domainName: string
    ): Promise<APIResponse<{ domain: string; allowed_sites: string[] }>> {
        return this.request<{ domain: string; allowed_sites: string[] }>(
            "GET",
            `/v1/domains/${domainName}/sites`
        );
    }

    /**
     * GET /v1/health — System health summary.
     */
    async getHealth(): Promise<APIResponse<HealthStatus>> {
        return this.request<HealthStatus>("GET", "/v1/health");
    }

    /**
     * Poll a request until completion or timeout.
     */
    async pollUntilComplete(
        requestId: string,
        intervalMs: number = 2000,
        maxAttempts: number = 60
    ): Promise<APIResponse<RequestStatus>> {
        for (let i = 0; i < maxAttempts; i++) {
            const response = await this.getRequestStatus(requestId);

            if (response.status === "completed" || response.status === "failed") {
                return response;
            }

            await new Promise((resolve) => setTimeout(resolve, intervalMs));
        }

        return {
            status: "timeout",
            data: null,
            error: { message: `Polling timed out after ${maxAttempts} attempts` },
        };
    }

    /**
     * GET /v1/requests/:id/fields — Get semantic fields for field selection.
     */
    async getSemanticFields(
        requestId: string
    ): Promise<APIResponse<{ sites: Record<string, { fields: SemanticField[] }> }>> {
        return this.request<{ sites: Record<string, { fields: SemanticField[] }> }>(
            "GET",
            `/v1/requests/${requestId}/fields`
        );
    }

    /**
     * POST /v1/requests/:id/select-fields — Submit field selection.
     */
    async submitFieldSelection(
        requestId: string,
        selections: Record<string, string[]>
    ): Promise<APIResponse<{ sites: Record<string, { fields: SelectedFieldValue[]; field_count: number }> }>> {
        return this.request<{ sites: Record<string, { fields: SelectedFieldValue[]; field_count: number }> }>(
            "POST",
            `/v1/requests/${requestId}/select-fields`,
            { selections }
        );
    }

    // ============================================================
    // Private HTTP client
    // ============================================================

    private async request<T>(
        method: string,
        path: string,
        body?: unknown
    ): Promise<APIResponse<T>> {
        const url = `${this.baseUrl}${path}`;

        const headers: Record<string, string> = {
            Authorization: `Bearer ${this.token}`,
            "Content-Type": "application/json",
        };

        const options: RequestInit = {
            method,
            headers,
        };

        if (body && method !== "GET") {
            options.body = JSON.stringify(body);
        }

        try {
            const response = await fetch(url, options);

            if (response.status === 401) {
                throw new Error("Authentication failed — invalid or expired token");
            }

            if (response.status === 429) {
                const data = await response.json();
                throw new Error(
                    `Rate limit exceeded. Retry after ${data?.detail?.retry_after ?? "?"} seconds`
                );
            }

            if (response.status === 503) {
                throw new Error("Service temporarily unavailable — resources exhausted");
            }

            const data: APIResponse<T> = await response.json();
            return data;
        } catch (error) {
            if (error instanceof Error) {
                return {
                    status: "error",
                    data: null,
                    error: { message: error.message },
                };
            }
            return {
                status: "error",
                data: null,
                error: { message: "Unknown error occurred" },
            };
        }
    }
}

// ============================================================
// Types for Semantic Fields
// ============================================================

export interface SemanticField {
    raw_key: string;
    display_name: string;
    relevance: number;
    category: string;
    preview: string | null;
    engine: string;
}

export interface SelectedFieldValue {
    raw_key: string;
    display_name: string;
    value: string | null;
    category: string;
    engine: string;
}

// ============================================================
// Factory
// ============================================================

/**
 * Create a pre-configured UMSA API client.
 */
export function createClient(
    baseUrl: string = "http://localhost:8080",
    token: string = ""
): UMSAClient {
    return new UMSAClient(baseUrl, token);
}

