import React from 'react'
import { Link } from 'react-router-dom'
import { Disc, Activity, Database, Shield } from 'lucide-react'

const Layout: React.FC<{ children: React.ReactNode }> = ({ children }) => {
    return (
        <div className="min-h-screen">
            <nav className="glass-card" style={{ margin: '1rem', borderRadius: '1rem', padding: '1rem 2rem', display: 'flex', justifyContent: 'space-between', alignItems: 'center', position: 'sticky', top: '1rem', zIndex: 100 }}>
                <Link to="/" style={{ display: 'flex', alignItems: 'center', gap: '0.75rem', textDecoration: 'none', color: 'inherit' }}>
                    <div style={{ background: 'var(--primary)', padding: '0.5rem', borderRadius: '0.75rem' }}>
                        <Disc size={24} color="white" />
                    </div>
                    <div>
                        <h1 style={{ fontSize: '1.25rem', fontWeight: 800, letterSpacing: '-0.025em' }}>REVEALO</h1>
                        <p style={{ fontSize: '0.75rem', color: 'var(--text-dim)', fontWeight: 500 }}>REVEALO</p>
                    </div>
                </Link>
                <div style={{ display: 'flex', gap: '2rem', alignItems: 'center' }}>
                    <Link to="/" style={{ color: 'var(--text-dim)', textDecoration: 'none', fontSize: '0.875rem', fontWeight: 600 }}>Dashboard</Link>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', background: 'rgba(16, 185, 129, 0.1)', padding: '0.5rem 1rem', borderRadius: '2rem', border: '1px solid rgba(16, 185, 129, 0.2)' }}>
                        <div style={{ width: 8, height: 8, borderRadius: '50%', background: 'var(--success)', boxShadow: '0 0 8px var(--success)' }}></div>
                        <span style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--success)' }}>SYSTEM HEALTHY</span>
                    </div>
                </div>
            </nav>
            <main className="container animate-fade-in">
                {children}
            </main>
            <footer style={{ marginTop: '4rem', padding: '3rem', textAlign: 'center', borderTop: '1px solid var(--border)' }}>
                <p style={{ color: 'var(--text-dim)', fontSize: '0.875rem' }}>
                    &copy; 2026 Enterprise Unified Domain-Extensible MCP System. v2.0 Hardened.
                </p>
            </footer>
        </div>
    )
}

export default Layout
