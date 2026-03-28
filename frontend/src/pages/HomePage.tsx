import React, { useState, useEffect } from 'react'
import { useNavigate } from 'react-router-dom'
import { Search, Flame, Film, ArrowRight, ShieldCheck, Zap, X, Plus, Globe, AlertTriangle } from 'lucide-react'
import { umsaApi } from '../api'

const HomePage: React.FC = () => {
    const [query, setQuery] = useState('')
    const [sites, setSites] = useState<string[]>([])
    const [siteInput, setSiteInput] = useState('')
    const [unify, setUnify] = useState(false)
    const [isLoading, setIsLoading] = useState(false)
    const [error, setError] = useState<string | null>(null)
    const [suggestedSites, setSuggestedSites] = useState<string[]>([])
    const [siteWarning, setSiteWarning] = useState<string | null>(null)
    const navigate = useNavigate()

    // Load suggested sites from API on mount (these are just suggestions, not restrictions)
    useEffect(() => {
        (async () => {
            try {
                const resp = await umsaApi.getDomainSites('movie')
                if (resp.data?.allowed_sites) {
                    setSuggestedSites(resp.data.allowed_sites)
                }
            } catch {
                setSuggestedSites([
                    'imdb.com', 'rottentomatoes.com', 'metacritic.com',
                    'themoviedb.org', 'letterboxd.com'
                ])
            }
        })()
    }, [])

    const siteLabel = (domain: string): string => {
        const labels: Record<string, string> = {
            'imdb.com': 'IMDb',
            'rottentomatoes.com': 'Rotten Tomatoes',
            'metacritic.com': 'Metacritic',
            'themoviedb.org': 'TMDb',
            'letterboxd.com': 'Letterboxd',
        }
        return labels[domain] || domain
    }

    const normalizeDomain = (s: string): string => {
        return s.trim().toLowerCase()
            .replace(/^https?:\/\//, '')
            .replace(/^www\./, '')
            .replace(/\/.*$/, '')
    }

    const addSite = (domain: string) => {
        const normalized = normalizeDomain(domain)
        if (!normalized) return

        // Basic format validation — must contain a dot (i.e. be a real domain)
        if (!normalized.includes('.')) {
            setSiteWarning(
                `"${normalized}" doesn't look like a valid domain. Try the full domain, e.g. "bookmyshow.com" instead of "bms".`
            )
            return
        }

        setSiteWarning(null)
        if (!sites.includes(normalized)) {
            setSites([...sites, normalized])
        }
        setSiteInput('')
    }

    const removeSite = (domain: string) => {
        setSites(sites.filter(s => s !== domain))
    }

    const handleSiteKeyDown = (e: React.KeyboardEvent) => {
        if (e.key === 'Enter') {
            e.preventDefault()
            if (siteInput.trim()) addSite(siteInput.trim())
        }
    }

    const handleSubmit = async (e: React.FormEvent) => {
        e.preventDefault()
        if (!query.trim()) return
        if (sites.length === 0) {
            setError('Please add at least one target site.')
            return
        }

        setError(null)
        setIsLoading(true)
        try {
            const response = await umsaApi.submitScrape({ query, sites, domain: 'movie', unify })
            if (response.data?.request_id) {
                navigate(`/requests/${response.data.request_id}`)
            }
        } catch (error: any) {
            console.error('Submission failed:', error)
            const msg = error?.response?.data?.error?.message || error?.message || 'Failed to submit scrape request.'
            setError(msg)
        } finally {
            setIsLoading(false)
        }
    }

    return (
        <div className="animate-fade-in">
            <header className="header" style={{ textAlign: 'center', marginBottom: '4rem' }}>
                <div style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem', background: 'var(--glass)', padding: '0.5rem 1rem', borderRadius: '2rem', marginBottom: '1.5rem', border: '1px solid var(--border)' }}>
                    <Flame size={16} color="#f59e0b" />
                    <span style={{ fontSize: '0.75rem', fontWeight: 700, letterSpacing: '0.05em' }}>PHASE 1 DOMAIN: MOVIES</span>
                </div>
                <h1 style={{ fontSize: '3.5rem', fontWeight: 900, marginBottom: '1rem', letterSpacing: '-0.04em', background: 'linear-gradient(to bottom, #fff, #94a3b8)', WebkitBackgroundClip: 'text', WebkitTextFillColor: 'transparent' }}>
                    Revealo
                </h1>
                <p style={{ fontSize: '1.25rem', color: 'var(--text-dim)', maxWidth: '600px', margin: '0 auto' }}>
                    Discover, Extract, and Unify Web Data Instantly.
                </p>
            </header>

            <section className="glass-card" style={{ maxWidth: '800px', margin: '0 auto 4rem auto', padding: '2.5rem' }}>
                <form onSubmit={handleSubmit}>
                    {/* Query input */}
                    <div style={{ marginBottom: '1.5rem' }}>
                        <label style={{ display: 'block', marginBottom: '0.75rem', fontWeight: 600, color: 'var(--text-dim)', fontSize: '0.875rem' }}>
                            What would you like to scrape?
                        </label>
                        <div style={{ position: 'relative' }}>
                            <input
                                type="text"
                                className="input"
                                placeholder="e.g., Inception (2010), Christopher Nolan films, Top Action 2024..."
                                value={query}
                                onChange={(e) => setQuery(e.target.value)}
                                style={{ paddingLeft: '3rem', fontSize: '1.125rem' }}
                                disabled={isLoading}
                            />
                            <Search style={{ position: 'absolute', left: '1rem', top: '50%', transform: 'translateY(-50%)', color: 'var(--text-dim)' }} size={20} />
                        </div>
                    </div>

                    {/* Sites input */}
                    <div style={{ marginBottom: '1.5rem' }}>
                        <label style={{ display: 'block', marginBottom: '0.5rem', fontWeight: 600, color: 'var(--text-dim)', fontSize: '0.875rem' }}>
                            <Globe size={14} style={{ display: 'inline', marginRight: '0.25rem', verticalAlign: 'middle' }} />
                            Target Sites
                        </label>
                        <p style={{ fontSize: '0.7rem', color: 'var(--text-dim)', margin: '0 0 0.75rem 0', opacity: 0.7 }}>
                            Quick-add popular sites or type any movie-related domain below.
                        </p>

                        {/* Quick-add chips — suggestions only */}
                        <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', marginBottom: '0.75rem' }}>
                            {suggestedSites.map(domain => {
                                const norm = normalizeDomain(domain)
                                const selected = sites.includes(norm)
                                return (
                                    <button
                                        key={domain}
                                        type="button"
                                        onClick={() => addSite(domain)}
                                        disabled={selected}
                                        style={{
                                            padding: '0.35rem 0.75rem',
                                            borderRadius: '2rem',
                                            border: '1px solid var(--border)',
                                            background: selected ? 'var(--primary)' : 'var(--glass)',
                                            color: selected ? '#fff' : 'var(--text)',
                                            fontSize: '0.75rem',
                                            fontWeight: 600,
                                            cursor: selected ? 'default' : 'pointer',
                                            opacity: selected ? 0.6 : 1,
                                            transition: 'all 0.2s',
                                        }}
                                    >
                                        {selected ? '✓ ' : '+ '}{siteLabel(domain)}
                                    </button>
                                )
                            })}
                        </div>

                        {/* Manual site input */}
                        <div style={{ display: 'flex', gap: '0.5rem' }}>
                            <input
                                type="text"
                                className="input"
                                placeholder="Any domain, e.g. bookmyshow.com, filmfreeway.com..."
                                value={siteInput}
                                onChange={(e) => { setSiteInput(e.target.value); setSiteWarning(null) }}
                                onKeyDown={handleSiteKeyDown}
                                disabled={isLoading}
                                style={{ flex: 1, fontSize: '0.875rem' }}
                            />
                            <button
                                type="button"
                                onClick={() => { if (siteInput.trim()) addSite(siteInput.trim()) }}
                                disabled={!siteInput.trim()}
                                className="btn"
                                style={{ padding: '0.5rem 1rem', fontSize: '0.875rem' }}
                            >
                                <Plus size={16} />
                            </button>
                        </div>

                        {/* Site format warning */}
                        {siteWarning && (
                            <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', padding: '0.5rem 0.75rem', borderRadius: '0.5rem', background: 'rgba(245, 158, 11, 0.1)', border: '1px solid rgba(245, 158, 11, 0.3)', color: '#f59e0b', fontSize: '0.8rem', marginTop: '0.5rem' }}>
                                <AlertTriangle size={14} style={{ flexShrink: 0 }} />
                                {siteWarning}
                            </div>
                        )}

                        {/* Selected sites tags */}
                        {sites.length > 0 && (
                            <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap', marginTop: '0.75rem' }}>
                                {sites.map(s => (
                                    <span
                                        key={s}
                                        style={{
                                            display: 'inline-flex', alignItems: 'center', gap: '0.25rem',
                                            padding: '0.25rem 0.5rem', borderRadius: '0.5rem',
                                            background: 'rgba(37, 99, 235, 0.15)', border: '1px solid rgba(37, 99, 235, 0.3)',
                                            color: 'var(--primary)', fontSize: '0.75rem', fontWeight: 600,
                                        }}
                                    >
                                        <Globe size={12} />{s}
                                        <button type="button" onClick={() => removeSite(s)} style={{ background: 'none', border: 'none', color: 'var(--error)', cursor: 'pointer', padding: 0, lineHeight: 1 }}>
                                            <X size={12} />
                                        </button>
                                    </span>
                                ))}
                            </div>
                        )}
                    </div>

                    {/* Unification toggle */}
                    <div style={{ marginBottom: '1.5rem' }}>
                        <div style={{
                            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                            padding: '1rem 1.25rem', borderRadius: '0.75rem',
                            background: unify ? 'rgba(124, 58, 237, 0.08)' : 'var(--glass)',
                            border: `1px solid ${unify ? 'rgba(124, 58, 237, 0.3)' : 'var(--border)'}`,
                            transition: 'all 0.3s ease',
                        }}>
                            <div>
                                <p style={{ fontWeight: 700, fontSize: '0.875rem', color: 'var(--text)', marginBottom: '0.25rem' }}>
                                    🔗 Cross-Site AI Unification
                                </p>
                                <p style={{ fontSize: '0.75rem', color: 'var(--text-dim)', lineHeight: 1.4 }}>
                                    Merges data from all sites using AI to resolve conflicts. Uses Phase 2 API.
                                </p>
                            </div>
                            <button
                                type="button"
                                onClick={() => setUnify(!unify)}
                                style={{
                                    width: '48px', height: '26px', borderRadius: '13px',
                                    background: unify ? 'linear-gradient(135deg, #7c3aed, #2563eb)' : 'rgba(255,255,255,0.15)',
                                    border: 'none', cursor: 'pointer', position: 'relative',
                                    transition: 'background 0.3s ease', flexShrink: 0,
                                }}
                            >
                                <div style={{
                                    width: '20px', height: '20px', borderRadius: '50%',
                                    background: 'white', position: 'absolute', top: '3px',
                                    left: unify ? '25px' : '3px',
                                    transition: 'left 0.3s ease',
                                    boxShadow: '0 1px 3px rgba(0,0,0,0.3)',
                                }} />
                            </button>
                        </div>
                    </div>
                    {error && (
                        <div style={{ padding: '0.75rem 1rem', borderRadius: '0.5rem', background: 'rgba(239, 68, 68, 0.1)', border: '1px solid rgba(239, 68, 68, 0.3)', color: 'var(--error)', fontSize: '0.875rem', marginBottom: '1rem' }}>
                            {error}
                        </div>
                    )}

                    <button type="submit" className="btn btn-primary" style={{ width: '100%', padding: '1rem', fontSize: '1.125rem' }} disabled={isLoading || !query.trim() || sites.length === 0}>
                        {isLoading ? 'INITIATING AGENT...' : (
                            <>
                                START SCRAPE PIPELINE <ArrowRight size={20} />
                            </>
                        )}
                    </button>
                </form>
            </section>

            <div className="grid grid-cols-2" style={{ maxWidth: '800px', margin: '0 auto' }}>
                <div className="glass-card" style={{ padding: '1.5rem', display: 'flex', gap: '1rem' }}>
                    <div style={{ background: 'rgba(37, 99, 235, 0.1)', padding: '0.75rem', borderRadius: '0.75rem', alignSelf: 'flex-start' }}>
                        <ShieldCheck size={24} color="var(--primary)" />
                    </div>
                    <div>
                        <h3 style={{ fontSize: '1rem', fontWeight: 700, marginBottom: '0.5rem' }}>Compliance First</h3>
                        <p style={{ fontSize: '0.875rem', color: 'var(--text-dim)', lineHeight: 1.5 }}>
                            Strict robots.txt enforcement and rate limiting protect host ecosystem integrity.
                        </p>
                    </div>
                </div>
                <div className="glass-card" style={{ padding: '1.5rem', display: 'flex', gap: '1rem' }}>
                    <div style={{ background: 'rgba(16, 185, 129, 0.1)', padding: '0.75rem', borderRadius: '0.75rem', alignSelf: 'flex-start' }}>
                        <Zap size={24} color="var(--success)" />
                    </div>
                    <div>
                        <h3 style={{ fontSize: '1rem', fontWeight: 700, marginBottom: '0.5rem' }}>Memory-First</h3>
                        <p style={{ fontSize: '0.875rem', color: 'var(--text-dim)', lineHeight: 1.5 }}>
                            Intelligent caching and partial reuse of pipelines significantly reduce latency.
                        </p>
                    </div>
                </div>
            </div>
        </div>
    )
}

export default HomePage
