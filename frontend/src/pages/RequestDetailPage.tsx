import React, { useEffect, useState } from 'react'
import { useParams, Link } from 'react-router-dom'
import {
    Loader2,
    CheckCircle2,
    XCircle,
    Clock,
    Database,
    Globe,
    ArrowLeft,
    ChevronRight,
    Code,
    Layout as LayoutIcon,
    AlertTriangle,
    Activity,
    Filter,
    Eye,
    Check,
    Download,
    ChevronDown,
    ChevronUp
} from 'lucide-react'
import { umsaApi } from '../api'
import type { SemanticField, SelectedFieldValue } from '../api/client'

// Ordered list of pipeline states — used to determine which steps are "done"
const STATE_ORDER = [
    'INIT',
    'INTENT_DONE',
    'VALIDATED',
    'RELEVANCE_DONE',
    'PIPELINES_RUNNING',
    'EXTRACTION_DONE',
    'UNIFIED',
    'COMPLETED',
]

function getStateIndex(state: string): number {
    const idx = STATE_ORDER.indexOf(state)
    return idx >= 0 ? idx : -1
}

const CATEGORY_COLORS: Record<string, string> = {
    identity: '#3b82f6',
    metadata: '#8b5cf6',
    rating: '#f59e0b',
    media: '#06b6d4',
    description: '#10b981',
    cast: '#ec4899',
    other: '#6b7280',
}

const RequestDetailPage: React.FC = () => {
    const { requestId } = useParams<{ requestId: string }>()
    const safeRequestId = requestId ?? ''
    const [request, setRequest] = useState<any>(null)
    const [isLoading, setIsLoading] = useState(true)
    const [error, setError] = useState<string | null>(null)
    const [activeSite, setActiveSite] = useState<string>('')

    // Semantic field selection state
    const [semanticFields, setSemanticFields] = useState<Record<string, { fields: SemanticField[] }> | null>(null)
    const [fieldSelections, setFieldSelections] = useState<Record<string, Set<string>>>({})
    const [selectedResults, setSelectedResults] = useState<Record<string, { fields: SelectedFieldValue[]; field_count: number }> | null>(null)
    const [isLoadingFields, setIsLoadingFields] = useState(false)
    const [isSubmitting, setIsSubmitting] = useState(false)
    const [fieldPhase, setFieldPhase] = useState<'loading' | 'select' | 'results'>('loading')
    const [expandedFields, setExpandedFields] = useState<Set<string>>(new Set())

    // ── Download helpers ──
    const downloadCSV = (siteName: string, fields: any[]) => {
        const rows = [['Field', 'Value', 'Category', 'Source']]
        fields.forEach(f => {
            const val = (f.value || '').replace(/"/g, '""')
            rows.push([f.display_name, `"${val}"`, f.category || '', f.engine || ''])
        })
        const csv = rows.map(r => r.join(',')).join('\n')
        const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
        const url = URL.createObjectURL(blob)
        const a = document.createElement('a')
        a.href = url
        a.download = `${siteName.replace(/\./g, '_')}_data.csv`
        a.click()
        URL.revokeObjectURL(url)
    }

    const downloadExcel = async (siteName: string, fields: any[]) => {
        // @ts-ignore
        const ExcelJS = (await import('exceljs')).default || (await import('exceljs'))
        const { saveAs } = await import('file-saver')

        const workbook = new ExcelJS.Workbook()
        const worksheet = workbook.addWorksheet(siteName.replace(/\./g, '_').slice(0, 31))

        // Define columns
        worksheet.columns = [
            { header: 'Field', key: 'Field', width: 25 },
            { header: 'Value', key: 'Value', width: 50 },
            { header: 'Category', key: 'Category', width: 15 },
            { header: 'Source', key: 'Source', width: 20 },
        ]

        // Add data
        fields.forEach(f => {
            worksheet.addRow({
                Field: f.display_name,
                Value: f.value || '',
                Category: f.category || '',
                Source: f.engine || '',
            })
        })

        // Style headers
        worksheet.getRow(1).font = { bold: true }

        // Generate buffer
        const buffer = await workbook.xlsx.writeBuffer()
        const blob = new Blob([buffer], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
        saveAs(blob, `${siteName.replace(/\./g, '_')}_data.xlsx`)
    }

    const fetchStatus = async () => {
        if (!safeRequestId) return
        try {
            const response = await umsaApi.getRequestStatus(safeRequestId)
            if (response.data) {
                setRequest(response.data)
                setIsLoading(false)

                // Polling if not terminal
                if (!['COMPLETED', 'FAILED'].includes(response.data.state || '')) {
                    setTimeout(fetchStatus, 2000)
                } else if (response.data.state === 'COMPLETED') {
                    const resultData: any = response.data.result || {}
                    const unifiedData = resultData.unified_data || resultData
                    const isSingleFinal = (resultData.cache_hit === true) || (unifiedData && unifiedData.name && !unifiedData.is_multi_item)

                    if (isSingleFinal && !unifiedData.is_multi_item && Object.keys(unifiedData).length > 0) {
                        // Bypass field selection and render unified_data directly
                        const sources = resultData.source_sites || ['Unified']
                        const siteName = sources[0] || 'Unified'
                        const fields: any[] = []

                        for (const [k, v] of Object.entries(unifiedData)) {
                            if (k.startsWith('_') || k === '@type' || v == null || v === '') continue

                            let displayVal = String(v)
                            if (Array.isArray(v)) {
                                displayVal = v.map((item: any) => typeof item === 'object' && item.name ? item.name : String(item)).join(', ')
                            } else if (typeof v === 'object' && v !== null) {
                                if ('name' in v) displayVal = String((v as any).name)
                                else if ('ratingValue' in v) displayVal = `${(v as any).ratingValue} / ${(v as any).bestRating || 10}`
                                else displayVal = JSON.stringify(v)
                            }

                            fields.push({
                                raw_key: k,
                                display_name: k.charAt(0).toUpperCase() + k.slice(1).replace(/_/g, ' '),
                                value: displayVal,
                                category: 'entity_detail',
                                engine: resultData.cache_hit ? 'cache_hit' : 'unified'
                            })
                        }
                        
                        setSelectedResults({
                            [siteName]: { fields, field_count: fields.length }
                        })
                        if (!activeSite) setActiveSite(siteName)
                        setFieldPhase('results')
                    } else {
                        // Fetch semantic fields when completed
                        fetchSemanticFields()
                    }
                }
            }
        } catch (err) {
            console.error('Failed to fetch status:', err)
            setError('Connection to backend lost. Retrying...')
            setTimeout(fetchStatus, 5000)
        }
    }

    const fetchSemanticFields = async () => {
        if (!safeRequestId) return
        setIsLoadingFields(true)
        try {
            const resp = await umsaApi.getSemanticFields(safeRequestId)
            if (resp.data?.sites) {
                setSemanticFields(resp.data.sites)
                // Pre-select all fields with relevance >= 0.7
                const selMap: Record<string, Set<string>> = {}
                for (const [site, data] of Object.entries(resp.data.sites)) {
                    selMap[site] = new Set(
                        data.fields
                            .filter((f: SemanticField) => f.relevance >= 0.7)
                            .map((f: SemanticField) => f.raw_key)
                    )
                }
                setFieldSelections(selMap)
                // Set active site to first site
                const firstSite = Object.keys(resp.data.sites)[0]
                if (firstSite && !activeSite) setActiveSite(firstSite)
                setFieldPhase('select')
            }
        } catch (err) {
            console.error('Failed to fetch semantic fields:', err)
        }
        setIsLoadingFields(false)
    }

    const toggleFieldSelection = (site: string, rawKey: string) => {
        setFieldSelections(prev => {
            const updated = { ...prev }
            const siteSet = new Set(updated[site] || [])
            if (siteSet.has(rawKey)) {
                siteSet.delete(rawKey)
            } else {
                siteSet.add(rawKey)
            }
            updated[site] = siteSet
            return updated
        })
    }

    const selectAllFields = (site: string) => {
        if (!semanticFields?.[site]) return
        setFieldSelections(prev => ({
            ...prev,
            [site]: new Set(semanticFields[site].fields.map(f => f.raw_key))
        }))
    }

    const deselectAllFields = (site: string) => {
        setFieldSelections(prev => ({
            ...prev,
            [site]: new Set()
        }))
    }

    const submitFieldSelection = async () => {
        if (!safeRequestId) return
        setIsSubmitting(true)
        try {
            const selections: Record<string, string[]> = {}
            for (const [site, keys] of Object.entries(fieldSelections)) {
                if (keys.size > 0) {
                    selections[site] = Array.from(keys)
                }
            }
            const resp = await umsaApi.submitFieldSelection(safeRequestId, selections)
            if (resp.data?.sites) {
                setSelectedResults(resp.data.sites)
                setFieldPhase('results')
            }
        } catch (err) {
            console.error('Failed to submit field selection:', err)
        }
        setIsSubmitting(false)
    }

    useEffect(() => {
        fetchStatus()
    }, [safeRequestId])

    if (isLoading && !request) {
        return (
            <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', minHeight: '50vh' }}>
                <Loader2 className="animate-spin" size={48} color="var(--primary)" />
                <p style={{ marginTop: '1rem', color: 'var(--text-dim)', fontWeight: 600 }}>CONNECTING TO SCRAPER ENGINE...</p>
            </div>
        )
    }

    const getStatusBadge = (state: string) => {
        switch (state) {
            case 'COMPLETED': return <span className="badge badge-completed">COMPLETED</span>
            case 'FAILED': return <span className="badge badge-failed">FAILED</span>
            case 'INIT': return <span className="badge badge-init">INITIALIZING</span>
            default: return <span className="badge badge-running">PIPELINES RUNNING</span>
        }
    }

    // Determine which step the request reached / failed at
    const currentState = request?.state || 'INIT'
    const isFailed = currentState === 'FAILED'
    const isCompleted = currentState === 'COMPLETED'
    const currentStateIdx = getStateIndex(currentState)

    // For FAILED, figure out the last good state by checking pipelines & error
    const getFailedAtIndex = (): number => {
        if (!isFailed) return -1
        const err = request?.error
        const failureClass = err?.failure_class || ''

        if (failureClass === 'CLARIFICATION_REQUIRED') {
            return STATE_ORDER.indexOf('INTENT_DONE')
        }
        if (failureClass === 'INVALID_SITE') {
            return STATE_ORDER.indexOf('VALIDATED')
        }
        if (failureClass === 'ROBOTS_BLOCKED' || failureClass === 'BOT_PROTECTION' ||
            failureClass === 'PIPELINES_FAILED' || failureClass === 'NO_VALID_URL' ||
            failureClass === 'ALL_SITES_BLOCKED' || failureClass === 'NETWORK_TIMEOUT' ||
            failureClass === 'EXTRACTION_SCHEMA_FAIL' || failureClass === 'DOM_STRUCTURE_CHANGED') {
            return STATE_ORDER.indexOf('PIPELINES_RUNNING')
        }
        if (failureClass === 'VALIDATION_FAILED') {
            return STATE_ORDER.indexOf('VALIDATED')
        }

        const pipelines = request?.pipelines || []
        if (pipelines.length > 0) {
            return STATE_ORDER.indexOf('PIPELINES_RUNNING')
        }

        const msg = (err?.message || '').toLowerCase()
        if (msg.includes('intent') || msg.includes('clarification')) return STATE_ORDER.indexOf('INTENT_DONE')
        if (msg.includes('validation') || msg.includes('policy') || msg.includes('site')) return STATE_ORDER.indexOf('VALIDATED')

        return STATE_ORDER.indexOf('INIT')
    }

    const failedAtIdx = getFailedAtIndex()

    const steps = [
        { id: 'INIT', label: 'Initialization', desc: 'Validating request & loading domain config' },
        { id: 'INTENT_DONE', label: 'Intent + Actionability', desc: 'AI intent parsing + actionability gate (≥0.75 confidence)' },
        { id: 'VALIDATED', label: 'Site Validation', desc: 'Validating user-provided sites against domain whitelist' },
        { id: 'RELEVANCE_DONE', label: 'Sites Confirmed', desc: 'Validated sites ready for pipeline execution' },
        { id: 'PIPELINES_RUNNING', label: 'Active Pipelines', desc: 'URL gen → robots gate → scraping → semantic matching' },
        { id: 'UNIFIED', label: 'Cross-Site Sync', desc: 'Tier-2 conflict resolution & merger' },
        { id: 'COMPLETED', label: 'Finalization', desc: 'Persisting validated unified record' },
    ]

    // ── Render field selection panel ──
    const renderFieldSelection = () => {
        if (!semanticFields) return null

        const siteNames = Object.keys(semanticFields)
        const currentSite = activeSite && siteNames.includes(activeSite) ? activeSite : siteNames[0] || ''
        const siteFields = semanticFields[currentSite]?.fields || []
        const selectedCount = fieldSelections[currentSite]?.size || 0

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <h4 style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-dim)', letterSpacing: '0.05em', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                        <Filter size={14} /> FIELD SELECTION
                    </h4>
                    <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>
                        {selectedCount} of {siteFields.length} fields selected
                    </span>
                </div>

                {/* Site tabs */}
                <div style={{ display: 'flex', gap: '0', borderRadius: '0.5rem', overflow: 'hidden', border: '1px solid var(--border)' }}>
                    {siteNames.map(site => (
                        <button
                            key={site}
                            onClick={() => setActiveSite(site)}
                            style={{
                                flex: 1,
                                padding: '0.5rem 1rem',
                                fontSize: '0.75rem',
                                fontWeight: 700,
                                cursor: 'pointer',
                                border: 'none',
                                background: currentSite === site ? 'rgba(37, 99, 235, 0.15)' : 'transparent',
                                color: currentSite === site ? 'var(--primary)' : 'var(--text-dim)',
                                borderBottom: currentSite === site ? '2px solid var(--primary)' : '2px solid transparent',
                                transition: 'all 0.2s ease',
                            }}
                        >
                            <Globe size={12} style={{ display: 'inline', verticalAlign: 'middle', marginRight: '0.25rem' }} />
                            {site}
                        </button>
                    ))}
                </div>

                {/* Select all / deselect all */}
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <button
                        onClick={() => selectAllFields(currentSite)}
                        style={{
                            padding: '0.25rem 0.75rem', fontSize: '0.7rem', fontWeight: 600,
                            background: 'rgba(16, 185, 129, 0.1)', color: 'var(--success)',
                            border: '1px solid rgba(16, 185, 129, 0.3)', borderRadius: '0.375rem',
                            cursor: 'pointer',
                        }}
                    >
                        Select All
                    </button>
                    <button
                        onClick={() => deselectAllFields(currentSite)}
                        style={{
                            padding: '0.25rem 0.75rem', fontSize: '0.7rem', fontWeight: 600,
                            background: 'rgba(239, 68, 68, 0.1)', color: 'var(--error)',
                            border: '1px solid rgba(239, 68, 68, 0.3)', borderRadius: '0.375rem',
                            cursor: 'pointer',
                        }}
                    >
                        Deselect All
                    </button>
                </div>

                {/* Field checkboxes */}
                <div style={{
                    display: 'grid',
                    gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
                    gap: '0.5rem',
                    maxHeight: '360px',
                    overflowY: 'auto',
                    padding: '0.5rem',
                    borderRadius: '0.5rem',
                    border: '1px solid var(--border)',
                    background: 'rgba(255,255,255,0.02)',
                }}>
                    {siteFields.map(field => {
                        const isSelected = fieldSelections[currentSite]?.has(field.raw_key) || false
                        const catColor = CATEGORY_COLORS[field.category] || CATEGORY_COLORS.other

                        return (
                            <div
                                key={field.raw_key}
                                onClick={() => toggleFieldSelection(currentSite, field.raw_key)}
                                style={{
                                    display: 'flex',
                                    alignItems: 'flex-start',
                                    gap: '0.5rem',
                                    padding: '0.625rem 0.75rem',
                                    borderRadius: '0.5rem',
                                    border: `1px solid ${isSelected ? catColor + '44' : 'var(--border)'}`,
                                    background: isSelected ? catColor + '0a' : 'transparent',
                                    cursor: 'pointer',
                                    transition: 'all 0.15s ease',
                                }}
                            >
                                <div style={{
                                    width: '18px', height: '18px', borderRadius: '4px',
                                    border: `2px solid ${isSelected ? catColor : 'var(--text-dim)'}`,
                                    background: isSelected ? catColor : 'transparent',
                                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                                    flexShrink: 0, marginTop: '1px',
                                }}>
                                    {isSelected && <Check size={12} color="white" />}
                                </div>
                                <div style={{ flex: 1, minWidth: 0 }}>
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.375rem', flexWrap: 'wrap' }}>
                                        <span style={{ fontSize: '0.8rem', fontWeight: 700, color: 'var(--text)' }}>
                                            {field.display_name}
                                        </span>
                                        <span style={{
                                            fontSize: '0.6rem', padding: '0.0625rem 0.375rem',
                                            borderRadius: '2rem', background: catColor + '22',
                                            color: catColor, fontWeight: 600,
                                        }}>
                                            {field.category}
                                        </span>
                                    </div>
                                    <p style={{ fontSize: '0.675rem', color: 'var(--text-dim)', marginTop: '0.125rem' }}>
                                        {field.raw_key}
                                    </p>
                                    {field.preview && (
                                        <p style={{
                                            fontSize: '0.65rem', color: 'var(--text-dim)', marginTop: '0.25rem',
                                            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                                            opacity: 0.7,
                                        }}>
                                            {field.preview.substring(0, 80)}{field.preview.length > 80 ? '…' : ''}
                                        </p>
                                    )}
                                </div>
                                <span style={{
                                    fontSize: '0.65rem', fontWeight: 700,
                                    color: field.relevance >= 0.8 ? 'var(--success)' : field.relevance >= 0.5 ? '#f59e0b' : 'var(--text-dim)',
                                }}>
                                    {(field.relevance * 100).toFixed(0)}%
                                </span>
                            </div>
                        )
                    })}
                </div>

                {/* Submit button */}
                <button
                    onClick={submitFieldSelection}
                    disabled={isSubmitting || Object.values(fieldSelections).every(s => s.size === 0)}
                    style={{
                        padding: '0.75rem 1.5rem',
                        fontSize: '0.875rem',
                        fontWeight: 700,
                        background: isSubmitting ? 'var(--glass)' : 'linear-gradient(135deg, var(--primary), #7c3aed)',
                        color: 'white',
                        border: 'none',
                        borderRadius: '0.5rem',
                        cursor: isSubmitting ? 'not-allowed' : 'pointer',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'center',
                        gap: '0.5rem',
                        transition: 'all 0.2s ease',
                        opacity: Object.values(fieldSelections).every(s => s.size === 0) ? 0.5 : 1,
                    }}
                >
                    {isSubmitting ? (
                        <><Loader2 size={16} className="animate-spin" /> Processing...</>
                    ) : (
                        <><Eye size={16} /> Show Selected Fields</>
                    )}
                </button>
            </div>
        )
    }

    // ── Render field results ──
    const renderFieldResults = () => {
        if (!selectedResults) return null

        const siteNames = Object.keys(selectedResults)
        const currentSite = activeSite && siteNames.includes(activeSite) ? activeSite : siteNames[0] || ''
        const siteData = selectedResults[currentSite]

        const toggleExpand = (key: string) => {
            setExpandedFields(prev => {
                const next = new Set(prev)
                next.has(key) ? next.delete(key) : next.add(key)
                return next
            })
        }

        return (
            <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                    <h4 style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-dim)', letterSpacing: '0.05em', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                        <Database size={14} /> EXTRACTED DATA
                    </h4>
                    <button
                        onClick={() => setFieldPhase('select')}
                        style={{
                            padding: '0.25rem 0.75rem', fontSize: '0.7rem', fontWeight: 600,
                            background: 'rgba(37, 99, 235, 0.1)', color: 'var(--primary)',
                            border: '1px solid rgba(37, 99, 235, 0.3)', borderRadius: '0.375rem',
                            cursor: 'pointer',
                        }}
                    >
                        ← Change Selection
                    </button>
                </div>

                {/* Site tabs */}
                <div style={{ display: 'flex', gap: '0', borderRadius: '0.5rem', overflow: 'hidden', border: '1px solid rgba(16, 185, 129, 0.3)' }}>
                    {siteNames.map(site => (
                        <button
                            key={site}
                            onClick={() => setActiveSite(site)}
                            style={{
                                flex: 1,
                                padding: '0.5rem 1rem',
                                fontSize: '0.75rem',
                                fontWeight: 700,
                                cursor: 'pointer',
                                border: 'none',
                                background: currentSite === site ? 'rgba(16, 185, 129, 0.15)' : 'transparent',
                                color: currentSite === site ? 'var(--success)' : 'var(--text-dim)',
                                borderBottom: currentSite === site ? '2px solid var(--success)' : '2px solid transparent',
                                transition: 'all 0.2s ease',
                            }}
                        >
                            <Globe size={12} style={{ display: 'inline', verticalAlign: 'middle', marginRight: '0.25rem' }} />
                            {site} ({selectedResults[site]?.field_count || 0})
                        </button>
                    ))}
                </div>

                {/* Download buttons */}
                {siteData && siteData.fields.length > 0 && (
                    <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                        <button
                            onClick={() => downloadCSV(currentSite, siteData.fields)}
                            style={{
                                padding: '0.375rem 0.875rem', fontSize: '0.7rem', fontWeight: 600,
                                background: 'rgba(16, 185, 129, 0.1)', color: 'var(--success)',
                                border: '1px solid rgba(16, 185, 129, 0.3)', borderRadius: '0.375rem',
                                cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '0.375rem',
                                transition: 'all 0.2s ease',
                            }}
                        >
                            <Download size={13} /> CSV
                        </button>
                        <button
                            onClick={() => downloadExcel(currentSite, siteData.fields)}
                            style={{
                                padding: '0.375rem 0.875rem', fontSize: '0.7rem', fontWeight: 600,
                                background: 'rgba(37, 99, 235, 0.1)', color: 'var(--primary)',
                                border: '1px solid rgba(37, 99, 235, 0.3)', borderRadius: '0.375rem',
                                cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '0.375rem',
                                transition: 'all 0.2s ease',
                            }}
                        >
                            <Download size={13} /> Excel
                        </button>
                    </div>
                )}

                {/* Results — Card-based layout */}
                {siteData && siteData.fields.length > 0 ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                        {siteData.fields.map((field, idx) => {
                            const catColor = CATEGORY_COLORS[field.category] || CATEGORY_COLORS.other
                            const val = field.value || ''
                            const isLong = val.length > 200
                            const isExpanded = expandedFields.has(`${currentSite}:${field.raw_key}`)
                            const displayVal = isLong && !isExpanded ? val.slice(0, 200) + '…' : val

                            return (
                                <div
                                    key={field.raw_key}
                                    style={{
                                        padding: '0.875rem 1rem',
                                        borderRadius: '0.625rem',
                                        border: '1px solid rgba(16, 185, 129, 0.15)',
                                        background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.02)',
                                        transition: 'border-color 0.2s',
                                    }}
                                >
                                    {/* Field header row */}
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem', marginBottom: '0.375rem' }}>
                                        <div style={{
                                            width: '7px', height: '7px', borderRadius: '50%',
                                            background: catColor, flexShrink: 0,
                                        }} />
                                        <span style={{ fontWeight: 700, fontSize: '0.8rem', color: 'var(--text)' }}>
                                            {field.display_name}
                                        </span>
                                        <span style={{
                                            fontSize: '0.575rem', padding: '0.0625rem 0.375rem',
                                            borderRadius: '2rem', background: catColor + '22',
                                            color: catColor, fontWeight: 600, flexShrink: 0, marginLeft: 'auto'
                                        }}>
                                            {field.category}
                                        </span>
                                    </div>

                                    {/* Value — full width, word-wrapped */}
                                    <div style={{
                                        fontSize: '0.825rem', color: 'var(--text)', lineHeight: 1.6,
                                        wordBreak: 'break-word', whiteSpace: 'pre-wrap',
                                        paddingLeft: '1.125rem',
                                    }}>
                                        {displayVal || <span style={{ color: 'var(--text-dim)', fontStyle: 'italic' }}>—</span>}
                                        {isLong && (
                                            <button
                                                onClick={() => toggleExpand(`${currentSite}:${field.raw_key}`)}
                                                style={{
                                                    background: 'none', border: 'none', cursor: 'pointer',
                                                    color: 'var(--primary)', fontSize: '0.7rem', fontWeight: 600,
                                                    display: 'inline-flex', alignItems: 'center', gap: '0.25rem',
                                                    marginLeft: '0.5rem', padding: '0.125rem 0',
                                                }}
                                            >
                                                {isExpanded ? <><ChevronUp size={12} /> Show less</> : <><ChevronDown size={12} /> Show more</>}
                                            </button>
                                        )}
                                    </div>
                                </div>
                            )
                        })}
                    </div>
                ) : (
                    <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-dim)' }}>
                        <p>No fields selected for this site.</p>
                    </div>
                )}
            </div>
        )
    }

    return (
        <div className="animate-fade-in">
            <Link to="/" style={{ display: 'inline-flex', alignItems: 'center', gap: '0.5rem', color: 'var(--text-dim)', textDecoration: 'none', marginBottom: '2rem', fontSize: '0.875rem', fontWeight: 600 }}>
                <ArrowLeft size={16} /> BACK TO DASHBOARD
            </Link>

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '3rem' }}>
                <div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '1rem', marginBottom: '0.5rem' }}>
                        <h1 style={{ fontSize: '2rem', fontWeight: 800 }}>Request Lifecycle</h1>
                        {getStatusBadge(request?.state)}
                    </div>
                    <p style={{ color: 'var(--text-dim)', fontFamily: 'monospace', fontSize: '0.875rem' }}>ID: {safeRequestId}</p>
                    {request?.query && (
                        <div style={{ marginTop: '1rem', padding: '0.75rem 1rem', background: 'var(--glass)', borderRadius: '0.5rem', border: '1px solid var(--border)', display: 'inline-block' }}>
                            <p style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-dim)', marginBottom: '0.25rem' }}>QUERY</p>
                            <p style={{ fontSize: '1rem', fontWeight: 600, color: 'var(--text)' }}>"{request.query}"</p>
                        </div>
                    )}
                </div>
                <div className="glass-card" style={{ padding: '0.75rem 1.5rem', display: 'flex', gap: '2rem' }}>
                    <div style={{ textAlign: 'center' }}>
                        <p style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-dim)', marginBottom: '0.25rem' }}>DOMAIN</p>
                        <p style={{ fontWeight: 800 }}>{request?.domain?.toUpperCase()}</p>
                    </div>
                    <div style={{ width: '1px', background: 'var(--border)' }}></div>
                    <div style={{ textAlign: 'center' }}>
                        <p style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-dim)', marginBottom: '0.25rem' }}>SCHEMA</p>
                        <p style={{ fontWeight: 800 }}>{request?.schema_version || '—'}</p>
                    </div>
                </div>
            </div>

            <div className="grid grid-cols-2" style={{ alignItems: 'start' }}>
                <div className="grid" style={{ gap: '1rem' }}>
                    <h2 style={{ fontSize: '1.25rem', fontWeight: 700, marginBottom: '0.5rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                        <Activity size={20} color="var(--primary)" /> Execution Trace
                    </h2>

                    {/* State Tracker */}
                    <div className="glass-card" style={{ padding: 0, overflow: 'hidden' }}>
                        {steps.map((step, idx, arr) => {
                            const stepIdx = getStateIndex(step.id)

                            let stepStatus: 'done' | 'failed' | 'active' | 'pending'

                            if (isCompleted) {
                                stepStatus = 'done'
                            } else if (isFailed) {
                                if (stepIdx < failedAtIdx) {
                                    stepStatus = 'done'
                                } else if (stepIdx === failedAtIdx) {
                                    stepStatus = 'failed'
                                } else {
                                    stepStatus = 'pending'
                                }
                            } else {
                                if (stepIdx < currentStateIdx) {
                                    stepStatus = 'done'
                                } else if (stepIdx === currentStateIdx) {
                                    stepStatus = 'active'
                                } else {
                                    stepStatus = 'pending'
                                }
                            }

                            const getIcon = () => {
                                switch (stepStatus) {
                                    case 'done':
                                        return (
                                            <div style={{
                                                width: '24px', height: '24px', borderRadius: '50%',
                                                background: 'var(--success)',
                                                display: 'flex', alignItems: 'center', justifyContent: 'center',
                                            }}>
                                                <CheckCircle2 size={16} color="white" />
                                            </div>
                                        )
                                    case 'failed':
                                        return (
                                            <div style={{
                                                width: '24px', height: '24px', borderRadius: '50%',
                                                background: 'var(--error)',
                                                display: 'flex', alignItems: 'center', justifyContent: 'center',
                                            }}>
                                                <XCircle size={16} color="white" />
                                            </div>
                                        )
                                    case 'active':
                                        return (
                                            <div style={{
                                                width: '24px', height: '24px', borderRadius: '50%',
                                                background: 'var(--primary)',
                                                display: 'flex', alignItems: 'center', justifyContent: 'center',
                                            }}>
                                                <Loader2 size={14} color="white" className="animate-spin" />
                                            </div>
                                        )
                                    default:
                                        return (
                                            <div style={{
                                                width: '24px', height: '24px', borderRadius: '50%',
                                                background: 'var(--glass)',
                                                display: 'flex', alignItems: 'center', justifyContent: 'center',
                                                border: '1px solid var(--border)',
                                            }}>
                                                <div style={{ width: 8, height: 8, borderRadius: '50%', background: 'var(--text-dim)' }}></div>
                                            </div>
                                        )
                                }
                            }

                            return (
                                <div key={step.id} style={{
                                    padding: '1.25rem 1.5rem',
                                    borderBottom: idx === arr.length - 1 ? 'none' : '1px solid var(--border)',
                                    display: 'flex',
                                    gap: '1.25rem',
                                    background: stepStatus === 'active' ? 'rgba(37, 99, 235, 0.05)'
                                        : stepStatus === 'failed' ? 'rgba(239, 68, 68, 0.05)'
                                            : 'transparent',
                                    opacity: stepStatus === 'pending' ? 0.4 : 1,
                                }}>
                                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', gap: '0.5rem' }}>
                                        {getIcon()}
                                    </div>
                                    <div>
                                        <h4 style={{ fontSize: '0.9375rem', fontWeight: 700, color: stepStatus === 'pending' ? 'var(--text-dim)' : 'var(--text)' }}>
                                            {step.label}
                                            {stepStatus === 'active' && <span style={{ marginLeft: '0.75rem', fontSize: '0.625rem', background: 'var(--primary)', color: 'white', padding: '0.125rem 0.375rem', borderRadius: '0.25rem' }}>ACTIVE</span>}
                                            {stepStatus === 'failed' && <span style={{ marginLeft: '0.75rem', fontSize: '0.625rem', background: 'var(--error)', color: 'white', padding: '0.125rem 0.375rem', borderRadius: '0.25rem' }}>FAILED</span>}
                                        </h4>
                                        <p style={{ fontSize: '0.75rem', color: 'var(--text-dim)', marginTop: '0.25rem' }}>{step.desc}</p>
                                    </div>
                                </div>
                            )
                        })}
                    </div>
                </div>

                <div className="grid">
                    <h2 style={{ fontSize: '1.25rem', fontWeight: 700, marginBottom: '0.5rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                        <Database size={20} color="var(--success)" /> Structured Results
                    </h2>

                    <div className="glass-card" style={{ minHeight: '400px', display: 'flex', flexDirection: 'column' }}>
                        {request?.state === 'FAILED' ? (
                            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '2rem', textAlign: 'center' }}>
                                <AlertTriangle size={48} color="var(--error)" style={{ marginBottom: '1rem' }} />
                                <h3 style={{ fontSize: '1.25rem', fontWeight: 700, marginBottom: '0.5rem' }}>Execution Failed</h3>
                                <p style={{ color: 'var(--text-dim)', fontSize: '0.875rem', maxWidth: '400px' }}>
                                    {request?.error?.message || 'Unknown system error occurred.'}
                                </p>
                                {request?.error?.failure_class && (
                                    <div style={{ marginTop: '1.5rem', padding: '1rem', background: 'rgba(239, 68, 68, 0.1)', border: '1px solid rgba(239, 68, 68, 0.2)', borderRadius: '0.5rem', width: '100%', textAlign: 'left', fontFamily: 'monospace', fontSize: '0.75rem' }}>
                                        CLASS: {request.error.failure_class}
                                    </div>
                                )}
                            </div>
                        ) : request?.state === 'COMPLETED' ? (
                            <div style={{ flex: 1, padding: '1.5rem', display: 'flex', flexDirection: 'column', gap: '1.5rem' }}>
                                {/* Results Wrapper */}
                                {(() => {
                                    const result = typeof request.result === 'string'
                                        ? (() => { try { return JSON.parse(request.result) } catch { return request.result } })()
                                        : request.result

                                    return (
                                        <>
                                            {/* Field Selection / Multi-Item Results */}
                                            {(() => {
                                                // Check for multi-item results
                                                const unifiedData = result?.unified_data || {}
                                                const isMultiItem = unifiedData?.is_multi_item === true
                                                const unifiedItems: any[] = unifiedData?.unified_items || []

                                                if (isMultiItem && unifiedItems.length > 0) {
                                                    // ── Multi-item site filtering ──
                                                    const allSources = Array.from(new Set(unifiedItems.flatMap(item => item._source_sites || []))).sort()
                                                    const currentListSite = activeSite || 'ALL'

                                                    const filteredItems = currentListSite === 'ALL'
                                                        ? unifiedItems
                                                        : unifiedItems.filter(item => (item._source_sites || []).includes(currentListSite))

                                                    const getItemTitle = (item: any): string => {
                                                        return item?._heading || item?.name || item?.title || item?.headline || item?._primary_link_text || '?'
                                                    }
                                                    const getExtraFields = (item: any): string[] => {
                                                        const skip = new Set(['_heading', 'name', 'title', 'headline', '_primary_link_text', '_position', 'source_url', 'source_site', '_source_sites', '_text_snippets', '_all_images', '_data_attributes', 'id', 'request_id'])
                                                        const extras: string[] = []
                                                        for (const [k, v] of Object.entries(item)) {
                                                            if (skip.has(k) || v == null || v === '' || (Array.isArray(v) && v.length === 0)) continue
                                                            const display = Array.isArray(v) ? v.slice(0, 5).join(', ') : String(v)
                                                            if (display.length > 80) continue
                                                            extras.push(`${k.replace(/^_/, '')}: ${display}`)
                                                            if (extras.length >= 3) break
                                                        }
                                                        return extras
                                                    }

                                                    return (
                                                        <div style={{ display: 'flex', flexDirection: 'column', gap: '1rem' }}>
                                                            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                                                                <h4 style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-dim)', letterSpacing: '0.05em', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                                    <Database size={14} /> EXTRACTED DATA
                                                                </h4>
                                                                <div style={{ display: 'flex', gap: '0.75rem', alignItems: 'center' }}>
                                                                    <span style={{ fontSize: '0.7rem', color: 'var(--text-dim)' }}>
                                                                        {unifiedData.total_items_raw || 0} raw → {unifiedItems.length} unique
                                                                    </span>
                                                                    <span style={{
                                                                        fontSize: '0.65rem', padding: '0.125rem 0.5rem',
                                                                        borderRadius: '2rem', background: 'rgba(37, 99, 235, 0.15)',
                                                                        color: 'var(--primary)', fontWeight: 700,
                                                                    }}>
                                                                        MULTI-ITEM
                                                                    </span>
                                                                </div>
                                                            </div>

                                                            {/* Site Tabs for Multi-Item */}
                                                            <div style={{ display: 'flex', gap: '0', borderRadius: '0.5rem', overflow: 'hidden', border: '1px solid var(--border)' }}>
                                                                <button
                                                                    onClick={() => setActiveSite('ALL')}
                                                                    style={{
                                                                        flex: 1,
                                                                        padding: '0.5rem 1rem',
                                                                        fontSize: '0.75rem',
                                                                        fontWeight: 700,
                                                                        cursor: 'pointer',
                                                                        border: 'none',
                                                                        background: currentListSite === 'ALL' ? 'rgba(37, 99, 235, 0.15)' : 'transparent',
                                                                        color: currentListSite === 'ALL' ? 'var(--primary)' : 'var(--text-dim)',
                                                                        borderBottom: currentListSite === 'ALL' ? '2px solid var(--primary)' : '2px solid transparent',
                                                                        transition: 'all 0.2s ease',
                                                                    }}
                                                                >
                                                                    ALL SITES ({unifiedItems.length})
                                                                </button>
                                                                {allSources.map(site => {
                                                                    const count = unifiedItems.filter(item => (item._source_sites || []).includes(site)).length
                                                                    return (
                                                                        <button
                                                                            key={site}
                                                                            onClick={() => setActiveSite(site)}
                                                                            style={{
                                                                                flex: 1,
                                                                                padding: '0.5rem 1rem',
                                                                                fontSize: '0.75rem',
                                                                                fontWeight: 700,
                                                                                cursor: 'pointer',
                                                                                border: 'none',
                                                                                background: currentListSite === site ? 'rgba(16, 185, 129, 0.15)' : 'transparent',
                                                                                color: currentListSite === site ? 'var(--success)' : 'var(--text-dim)',
                                                                                borderBottom: currentListSite === site ? '2px solid var(--success)' : '2px solid transparent',
                                                                                transition: 'all 0.2s ease',
                                                                            }}
                                                                        >
                                                                            <Globe size={12} style={{ display: 'inline', verticalAlign: 'middle', marginRight: '0.25rem' }} />
                                                                            {site.replace(/\.(com|org|net)$/, '')} ({count})
                                                                        </button>
                                                                    )
                                                                })}
                                                            </div>

                                                            {/* Download buttons for current list */}
                                                            <div style={{ display: 'flex', gap: '0.5rem', justifyContent: 'flex-end' }}>
                                                                <button
                                                                    onClick={() => downloadCSV(currentListSite === 'ALL' ? 'unified' : currentListSite, filteredItems.map(item => ({
                                                                        display_name: getItemTitle(item),
                                                                        value: getExtraFields(item).join(' | '),
                                                                        category: 'list_item',
                                                                        engine: (item._source_sites || []).join(', ')
                                                                    })))}
                                                                    style={{
                                                                        padding: '0.375rem 0.875rem', fontSize: '0.7rem', fontWeight: 600,
                                                                        background: 'rgba(16, 185, 129, 0.1)', color: 'var(--success)',
                                                                        border: '1px solid rgba(16, 185, 129, 0.3)', borderRadius: '0.375rem',
                                                                        cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '0.375rem',
                                                                        transition: 'all 0.2s ease',
                                                                    }}
                                                                >
                                                                    <Download size={13} /> CSV
                                                                </button>
                                                                <button
                                                                    onClick={() => downloadExcel(currentListSite === 'ALL' ? 'unified' : currentListSite, filteredItems.map(item => ({
                                                                        display_name: getItemTitle(item),
                                                                        value: getExtraFields(item).join(' | '),
                                                                        category: 'list_item',
                                                                        engine: (item._source_sites || []).join(', ')
                                                                    })))}
                                                                    style={{
                                                                        padding: '0.375rem 0.875rem', fontSize: '0.7rem', fontWeight: 600,
                                                                        background: 'rgba(37, 99, 235, 0.1)', color: 'var(--primary)',
                                                                        border: '1px solid rgba(37, 99, 235, 0.3)', borderRadius: '0.375rem',
                                                                        cursor: 'pointer', display: 'flex', alignItems: 'center', gap: '0.375rem',
                                                                        transition: 'all 0.2s ease',
                                                                    }}
                                                                >
                                                                    <Download size={13} /> Excel
                                                                </button>
                                                            </div>

                                                            <div style={{
                                                                borderRadius: '0.75rem', overflow: 'hidden',
                                                                border: '1px solid var(--border)',
                                                                maxHeight: '500px', overflowY: 'auto',
                                                                background: 'rgba(255,255,255,0.02)',
                                                            }}>
                                                                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem' }}>
                                                                    <thead>
                                                                        <tr style={{ background: 'var(--glass)', position: 'sticky', top: 0, zIndex: 1 }}>
                                                                            <th style={{ padding: '0.625rem 0.75rem', textAlign: 'center', fontWeight: 700, color: 'var(--text-dim)', fontSize: '0.7rem', borderBottom: '1px solid var(--border)', width: '40px' }}>#</th>
                                                                            <th style={{ padding: '0.625rem 0.75rem', textAlign: 'left', fontWeight: 700, color: 'var(--text-dim)', fontSize: '0.7rem', borderBottom: '1px solid var(--border)' }}>Title</th>
                                                                            <th style={{ padding: '0.625rem 0.75rem', textAlign: 'left', fontWeight: 700, color: 'var(--text-dim)', fontSize: '0.7rem', borderBottom: '1px solid var(--border)' }}>Details</th>
                                                                            <th style={{ padding: '0.625rem 0.75rem', textAlign: 'center', fontWeight: 700, color: 'var(--text-dim)', fontSize: '0.7rem', borderBottom: '1px solid var(--border)', width: '120px' }}>Sources</th>
                                                                        </tr>
                                                                    </thead>
                                                                    <tbody>
                                                                        {filteredItems.map((item: any, idx: number) => {
                                                                            const itemTitle = getItemTitle(item)
                                                                            const extras = getExtraFields(item)
                                                                            const sources: string[] = item._source_sites || []

                                                                            return (
                                                                                <tr key={idx} style={{
                                                                                    borderBottom: idx < filteredItems.length - 1 ? '1px solid var(--border)' : 'none',
                                                                                    background: idx % 2 === 0 ? 'transparent' : 'rgba(255,255,255,0.01)',
                                                                                }}>
                                                                                    <td style={{ padding: '0.5rem 0.75rem', textAlign: 'center', color: 'var(--text-dim)', fontWeight: 700, fontSize: '0.75rem' }}>
                                                                                        {idx + 1}
                                                                                    </td>
                                                                                    <td style={{ padding: '0.5rem 0.75rem', fontWeight: 700, color: 'var(--text)' }}>
                                                                                        {itemTitle.length > 60 ? itemTitle.slice(0, 57) + '...' : itemTitle}
                                                                                    </td>
                                                                                    <td style={{ padding: '0.5rem 0.75rem', color: 'var(--text-dim)', fontSize: '0.7rem' }}>
                                                                                        {extras.length > 0 ? extras.join(' · ') : <span style={{ fontStyle: 'italic', opacity: 0.5 }}>—</span>}
                                                                                    </td>
                                                                                    <td style={{ padding: '0.5rem 0.75rem', textAlign: 'center' }}>
                                                                                        <div style={{ display: 'flex', gap: '0.25rem', flexWrap: 'wrap', justifyContent: 'center' }}>
                                                                                            {sources.map((s: string) => (
                                                                                                <button
                                                                                                    key={s}
                                                                                                    onClick={() => setActiveSite(s)}
                                                                                                    style={{
                                                                                                        fontSize: '0.575rem', padding: '0.125rem 0.375rem',
                                                                                                        borderRadius: '2rem', background: 'rgba(16, 185, 129, 0.12)',
                                                                                                        border: '1px solid rgba(16, 185, 129, 0.25)',
                                                                                                        color: 'var(--success)', fontWeight: 600,
                                                                                                        cursor: 'pointer'
                                                                                                    }}
                                                                                                >
                                                                                                    {s.replace(/\.(com|org|net)$/, '')}
                                                                                                </button>
                                                                                            ))}
                                                                                        </div>
                                                                                    </td>
                                                                                </tr>
                                                                            )
                                                                        })}
                                                                    </tbody>
                                                                </table>
                                                                {filteredItems.length === 0 && (
                                                                    <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-dim)' }}>
                                                                        No items found for this site.
                                                                    </div>
                                                                )}
                                                            </div>
                                                        </div>
                                                    )
                                                }

                                                // ── Single-item: existing field selection / results ──
                                                if (isLoadingFields) {
                                                    return (
                                                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '2rem', color: 'var(--text-dim)' }}>
                                                            <Loader2 size={24} className="animate-spin" style={{ marginBottom: '0.5rem' }} />
                                                            <p style={{ fontSize: '0.8rem', fontWeight: 600 }}>Loading semantic fields...</p>
                                                        </div>
                                                    )
                                                }
                                                if (semanticFields && fieldPhase === 'select') {
                                                    return renderFieldSelection()
                                                }
                                                if (selectedResults && fieldPhase === 'results') {
                                                    return renderFieldResults()
                                                }
                                                if (!semanticFields) {
                                                    return (
                                                        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', padding: '2rem', color: 'var(--text-dim)' }}>
                                                            <p style={{ fontSize: '0.8rem', fontWeight: 600 }}>No semantic field data available.</p>
                                                            <p style={{ fontSize: '0.7rem', marginTop: '0.25rem' }}>Fields may still be processing.</p>
                                                        </div>
                                                    )
                                                }
                                                return null
                                            })()}

                                            {/* Sources */}
                                            {result?.sources && result.sources.length > 0 && (
                                                <div>
                                                    <h4 style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-dim)', marginBottom: '0.75rem', letterSpacing: '0.05em' }}>SOURCES USED</h4>
                                                    <div style={{ display: 'flex', gap: '0.5rem', flexWrap: 'wrap' }}>
                                                        {result.sources.map((s: string) => (
                                                            <span key={s} style={{ display: 'inline-flex', alignItems: 'center', gap: '0.25rem', padding: '0.25rem 0.75rem', borderRadius: '2rem', background: 'rgba(16, 185, 129, 0.15)', border: '1px solid rgba(16, 185, 129, 0.3)', fontSize: '0.75rem', fontWeight: 600, color: 'var(--success)' }}>
                                                                <CheckCircle2 size={12} />{s}
                                                            </span>
                                                        ))}
                                                    </div>
                                                </div>
                                            )}

                                            {/* Partial Failures */}
                                            {result?.partial_failures && result.partial_failures.length > 0 && (
                                                <div>
                                                    <h4 style={{ fontSize: '0.75rem', fontWeight: 700, color: 'var(--text-dim)', marginBottom: '0.75rem', letterSpacing: '0.05em' }}>PARTIAL FAILURES</h4>
                                                    <div style={{ display: 'flex', flexDirection: 'column', gap: '0.5rem' }}>
                                                        {result.partial_failures.map((pf: any, i: number) => (
                                                            <div key={i} style={{ padding: '0.75rem 1rem', background: 'rgba(239, 68, 68, 0.05)', borderRadius: '0.5rem', border: '1px solid rgba(239, 68, 68, 0.15)', display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '0.5rem' }}>
                                                                <div style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
                                                                    <XCircle size={14} color="var(--error)" />
                                                                    <span style={{ fontWeight: 700, fontSize: '0.8rem' }}>{pf.site_url}</span>
                                                                </div>
                                                                <span style={{ fontSize: '0.65rem', padding: '0.125rem 0.5rem', borderRadius: '2rem', background: 'rgba(239, 68, 68, 0.15)', color: 'var(--error)', fontWeight: 600, fontFamily: 'monospace' }}>
                                                                    {pf.failure_class}
                                                                </span>
                                                                <p style={{ fontSize: '0.7rem', color: 'var(--text-dim)', width: '100%' }}>{pf.error}</p>
                                                            </div>
                                                        ))}
                                                    </div>
                                                </div>
                                            )}
                                        </>
                                    )
                                })()}
                            </div>
                        ) : (
                            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', color: 'var(--text-dim)' }}>
                                <Clock size={32} style={{ marginBottom: '1rem', opacity: 0.5 }} />
                                <p style={{ fontSize: '0.875rem', fontWeight: 600 }}>Awaiting pipeline completion...</p>
                                <p style={{ fontSize: '0.75rem', marginTop: '0.25rem' }}>Partial results are being unified in memory.</p>
                            </div>
                        )}
                    </div>

                    <div style={{ marginTop: '1.5rem' }}>
                        <h3 style={{ fontSize: '0.875rem', fontWeight: 600, color: 'var(--text-dim)', marginBottom: '1rem' }}>SITES POLLED</h3>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.75rem' }}>
                            {(request?.pipelines || []).map((p: any, i: number) => {
                                let hostname = p.site_url
                                try { hostname = new URL(p.site_url).hostname } catch { /* keep raw */ }

                                const pipelineDone = ['EXTRACTED'].includes(p.state)
                                const pipelineFailed = ['FAILED', 'SKIPPED'].includes(p.state)

                                return (
                                    <div key={i} className="glass-card" style={{ padding: '0.5rem 1rem', display: 'flex', alignItems: 'center', gap: '0.5rem', fontSize: '0.75rem', fontWeight: 700 }}>
                                        <Globe size={14} color="var(--primary)" />
                                        {hostname}
                                        {pipelineDone
                                            ? <CheckCircle2 size={12} color="var(--success)" />
                                            : pipelineFailed
                                                ? <XCircle size={12} color="var(--error)" />
                                                : <Loader2 size={12} className="animate-spin" />
                                        }
                                    </div>
                                )
                            })}
                            {(!request?.pipelines || request.pipelines.length === 0) && (
                                <p style={{ fontSize: '0.75rem', color: 'var(--text-dim)', fontStyle: 'italic' }}>
                                    {isFailed
                                        ? 'No sites were reached before failure.'
                                        : currentState === 'INIT'
                                            ? 'Waiting for initialization...'
                                            : 'Site discovery in progress...'}
                                </p>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}

export default RequestDetailPage
