'use client';

import React, { useState } from 'react';
import { Stepper } from '@/components/ui/Stepper';
import { Card, CardContent, CardHeader, CardFooter } from '@/components/ui/Card';
import { Button } from '@/components/ui/Button';
import { FileDropZone } from './FileDropZone';
import { Input } from '@/components/ui/Input';
import {
    Database, FileJson, Share2, CheckCircle, XCircle, Loader2, AlertCircle,
    ShieldCheck, ShieldAlert, Shield, Sparkles, TrendingUp, AlertTriangle,
    CheckCircle2, Info, BarChart3, ChevronDown
} from 'lucide-react';
import { useToast } from '@/components/providers/ToastProvider';
import { apiClient } from '@/lib/apiClient';
import { useAuth } from '@/components/providers/AuthProvider';
import * as XLSX from 'xlsx';
import './ingestion.css';

// ─────────────────────────────────────────────────────────────
// Constants
// ─────────────────────────────────────────────────────────────

const STEPS = ['Select Source', 'Configure', 'Preview Schema', 'Validation Preview', 'Ingest'];

const CONNECTORS = [
    { id: 'postgres', name: 'PostgreSQL', type: 'Relational Database', icon: Database },
    { id: 'mongo', name: 'MongoDB', type: 'NoSQL Database', icon: FileJson },
    { id: 'api', name: 'REST API', type: 'HTTP Endpoint', icon: Share2 },
];

type EnforcementMode = 'strict' | 'warning' | 'monitor';

type ConnectorConfig = {
    pgHost?: string;
    pgPort?: string;
    pgDatabase?: string;
    pgUsername?: string;
    pgPassword?: string;
    pgTable?: string;
    mongoUri?: string;
    mongoDatabase?: string;
    mongoCollection?: string;
    apiUrl?: string;
    apiMethod?: string;
    apiHeaders?: string;
    apiBody?: string;
    pipelineName?: string;
};

interface ValidationReport {
    id: string;
    totalRows: number;
    validRows: number;
    invalidRows: number;
    passRate: number;
    overallScore: number;
    completeness: number;
    validity: number;
    uniqueness: number;
    errors: Array<{
        row: number;
        field: string;
        rule: string;
        expected: string;
        actual: string;
        severity: string;
    }>;
    summary: Record<string, number>;
}

interface PipelineResult {
    dataset: { id: string; name: string; status: string; storedRows: number };
    contract: { id: string; name: string; version: string; autoCreated: boolean };
    validationReport: ValidationReport;
    enforcementMode: EnforcementMode;
    status: string;
}

// ─────────────────────────────────────────────────────────────
// Helper: infer schema client-side (for preview only)
// ─────────────────────────────────────────────────────────────

function inferSchema(data: Record<string, any>[]): any[] {
    if (!data.length) return [];
    const first = data[0];
    return Object.keys(first).map(key => {
        const val = first[key];
        let type = 'String';
        if (val !== null && val !== undefined && val !== '') {
            if (!isNaN(Number(val))) {
                type = Number.isInteger(Number(val)) ? 'Integer' : 'Float';
            } else if (/^(true|false)$/i.test(String(val))) {
                type = 'Boolean';
            } else if (!isNaN(Date.parse(String(val)))) {
                type = 'Date';
            }
        }
        return { name: key, type, required: true, description: `Inferred from column '${key}'` };
    });
}

// ─────────────────────────────────────────────────────────────
// Main Component
// ─────────────────────────────────────────────────────────────

export default function IngestionPage() {
    const [currentStep, setCurrentStep] = useState(0);
    const [sourceType, setSourceType] = useState<'file' | 'connector' | null>(null);
    const [selectedConnector, setSelectedConnector] = useState<string | null>(null);
    const [selectedFile, setSelectedFile] = useState<File | null>(null);
    const [parsedFileData, setParsedFileData] = useState<Record<string, any>[]>([]);
    const [inferredSchema, setInferredSchema] = useState<any[]>([]);
    const [isInferring, setIsInferring] = useState(false);

    // Connector state
    const [connectorConfig, setConnectorConfig] = useState<ConnectorConfig>({
        pgPort: '5432',
        apiMethod: 'GET',
    });
    const [connectionStatus, setConnectionStatus] = useState<'idle' | 'testing' | 'success' | 'error'>('idle');
    const [connectionMessage, setConnectionMessage] = useState('');
    const [connectionMeta, setConnectionMeta] = useState<any>(null);
    const [isPulling, setIsPulling] = useState(false);
    const [pulledPreview, setPulledPreview] = useState<any[]>([]);
    const [pulledRowCount, setPulledRowCount] = useState(0);

    // Pipeline result (after schema inference step)
    const [pipelineResult, setPipelineResult] = useState<PipelineResult | null>(null);
    const [enforcementMode, setEnforcementMode] = useState<EnforcementMode>('monitor');
    const [isIngesting, setIsIngesting] = useState(false);
    const [ingestionDone, setIngestionDone] = useState(false);
    const [showAllErrors, setShowAllErrors] = useState(false);

    const { user } = useAuth();
    const { showToast } = useToast();

    const updateConfig = (key: keyof ConnectorConfig, value: string) => {
        setConnectorConfig(prev => ({ ...prev, [key]: value }));
    };

    // ─────────────────────────────────────────────────────────
    // Test Connection
    // ─────────────────────────────────────────────────────────

    const handleTestConnection = async () => {
        setConnectionStatus('testing');
        setConnectionMessage('');
        setConnectionMeta(null);
        try {
            let config: any = {};
            if (selectedConnector === 'postgres') {
                config = {
                    host: connectorConfig.pgHost,
                    port: connectorConfig.pgPort,
                    database: connectorConfig.pgDatabase,
                    username: connectorConfig.pgUsername,
                    password: connectorConfig.pgPassword,
                };
            } else if (selectedConnector === 'mongo') {
                config = { connectionUri: connectorConfig.mongoUri, database: connectorConfig.mongoDatabase };
            } else if (selectedConnector === 'api') {
                config = { url: connectorConfig.apiUrl, method: connectorConfig.apiMethod, headers: connectorConfig.apiHeaders };
            }

            const result = await apiClient.post('/data/connectors/test', {
                connectorType: selectedConnector,
                config,
            });

            if (result?.success) {
                setConnectionStatus('success');
                setConnectionMessage(result.message);
                setConnectionMeta(result);
            } else {
                setConnectionStatus('error');
                setConnectionMessage(result?.message || 'Connection failed');
            }
        } catch (err: any) {
            setConnectionStatus('error');
            setConnectionMessage(err.message || 'Connection test failed');
        }
    };

    // ─────────────────────────────────────────────────────────
    // Pull Data from Connector → runs full pipeline
    // ─────────────────────────────────────────────────────────

    const handlePullData = async (): Promise<boolean> => {
        setIsPulling(true);
        try {
            let config: any = {};
            if (selectedConnector === 'postgres') {
                config = {
                    host: connectorConfig.pgHost,
                    port: connectorConfig.pgPort,
                    database: connectorConfig.pgDatabase,
                    username: connectorConfig.pgUsername,
                    password: connectorConfig.pgPassword,
                    table: connectorConfig.pgTable,
                };
            } else if (selectedConnector === 'mongo') {
                config = {
                    connectionUri: connectorConfig.mongoUri,
                    database: connectorConfig.mongoDatabase,
                    collection: connectorConfig.mongoCollection,
                };
            } else if (selectedConnector === 'api') {
                config = {
                    url: connectorConfig.apiUrl,
                    method: connectorConfig.apiMethod,
                    headers: connectorConfig.apiHeaders,
                    body: connectorConfig.apiBody,
                };
            }

            const result = await apiClient.post('/data/connectors/pull', {
                connectorType: selectedConnector,
                config,
                pipelineName: connectorConfig.pipelineName,
                enforcementMode,
            });

            if (result?.schema) {
                setInferredSchema(result.schema);
                setPulledPreview(result.preview || []);
                setPulledRowCount(result.rowCount || 0);
                if (result.validationReport) {
                    setPipelineResult(result as PipelineResult);
                }
                return true;
            }
            showToast('Failed to pull data.', 'error');
            return false;
        } catch (err: any) {
            showToast(err.message || 'Failed to pull data from connector.', 'error');
            return false;
        } finally {
            setIsPulling(false);
        }
    };

    // ─────────────────────────────────────────────────────────
    // File upload → runs full pipeline
    // ─────────────────────────────────────────────────────────

    const handleFileUploadAndPipeline = async (): Promise<boolean> => {
        if (!selectedFile) return false;
        setIsInferring(true);
        try {
            const text = await selectedFile.text();
            let jsonData: Record<string, any>[] = [];

            if (selectedFile.name.toLowerCase().endsWith('.csv')) {
                const lines = text.split('\n').filter(l => l.trim().length > 0);
                const headers = lines[0].split(',').map(h => h.trim().replace(/^"|"$/g, ''));
                jsonData = lines.slice(1).map(line => {
                    const values = line.split(/,(?=(?:(?:[^"]*"){2})*[^"]*$)/);
                    return headers.reduce((obj, h, i) => {
                        obj[h] = values[i] ? values[i].replace(/^"|"$/g, '').trim() : '';
                        return obj;
                    }, {} as any);
                });
            } else if (selectedFile.name.toLowerCase().endsWith('.json')) {
                try { jsonData = JSON.parse(text); } catch { throw new Error('Invalid JSON format.'); }
            } else if (selectedFile.name.toLowerCase().match(/\.xlsx?$/)) {
                const arrayBuffer = await selectedFile.arrayBuffer();
                const workbook = XLSX.read(arrayBuffer, { type: 'array' });
                const sheetName = workbook.SheetNames[0];
                jsonData = XLSX.utils.sheet_to_json(workbook.Sheets[sheetName]);
            } else {
                throw new Error(`Unsupported file type: ${selectedFile.name}`);
            }

            if (!Array.isArray(jsonData)) jsonData = [jsonData];

            const schema = inferSchema(jsonData);
            setInferredSchema(schema);
            setParsedFileData(jsonData);

            // Run through the full pipeline
            const response = await apiClient.post('/data/datasets', {
                name: selectedFile.name,
                rawData: JSON.stringify(jsonData),
                inferredSchema: JSON.stringify(schema),
                source: 'file',
                sourceUri: selectedFile.name,
                enforcementMode,
            });

            if (!response) throw new Error('Failed to process dataset.');

            setPipelineResult(response as PipelineResult);
            showToast('Schema inferred and pipeline completed.', 'success');
            return true;
        } catch (err: any) {
            showToast(err.message || 'Failed to analyze file.', 'error');
            return false;
        } finally {
            setIsInferring(false);
        }
    };

    // ─────────────────────────────────────────────────────────
    // Step navigation
    // ─────────────────────────────────────────────────────────

    const handleNext = async () => {
        if (currentStep === 0 && !sourceType) {
            showToast('Please select a data source to continue.', 'error'); return;
        }
        if (currentStep === 0 && sourceType === 'connector' && !selectedConnector) {
            showToast('Please select a specific connector.', 'error'); return;
        }

        // Step 1 → 2: Parse file or pull from connector
        if (currentStep === 1) {
            if (sourceType === 'file' && selectedFile) {
                const ok = await handleFileUploadAndPipeline();
                if (!ok) return;
            } else if (sourceType === 'connector') {
                if (selectedConnector === 'postgres') {
                    if (!connectorConfig.pgHost || !connectorConfig.pgDatabase || !connectorConfig.pgUsername || !connectorConfig.pgTable) {
                        showToast('Please fill in all required PostgreSQL fields.', 'error'); return;
                    }
                } else if (selectedConnector === 'mongo') {
                    if (!connectorConfig.mongoUri || !connectorConfig.mongoCollection) {
                        showToast('Please fill in MongoDB URI and collection name.', 'error'); return;
                    }
                } else if (selectedConnector === 'api') {
                    if (!connectorConfig.apiUrl) {
                        showToast('Please provide the REST API URL.', 'error'); return;
                    }
                }
                const ok = await handlePullData();
                if (!ok) return;
            }
        }

        // Final step → finish
        if (currentStep === STEPS.length - 1) {
            await handleFinish();
            return;
        }

        setCurrentStep(prev => prev + 1);
    };

    const handleBack = () => {
        if (currentStep > 0) setCurrentStep(prev => prev - 1);
    };

    // ─────────────────────────────────────────────────────────
    // Finish: create workflow task
    // ─────────────────────────────────────────────────────────

    const handleFinish = async () => {
        setIsIngesting(true);
        try {
            const pipelineName = sourceType === 'file'
                ? selectedFile?.name
                : connectorConfig.pipelineName || selectedConnector;

            await apiClient.post('/data/workflows', {
                title: `Ingest ${pipelineName} Pipeline`,
                assignee: user?.name || 'System',
                status: 'Completed',
                progress: 100,
                category: 'Ingestion',
            });

            setIngestionDone(true);
            showToast('Ingestion pipeline completed successfully!', 'success');

            setTimeout(() => {
                setCurrentStep(0);
                setSourceType(null);
                setSelectedConnector(null);
                setSelectedFile(null);
                setInferredSchema([]);
                setParsedFileData([]);
                setPipelineResult(null);
                setEnforcementMode('monitor');
                setConnectorConfig({ pgPort: '5432', apiMethod: 'GET' });
                setConnectionStatus('idle');
                setConnectionMessage('');
                setConnectionMeta(null);
                setPulledPreview([]);
                setPulledRowCount(0);
                setIngestionDone(false);
                setShowAllErrors(false);
            }, 2500);
        } catch (err) {
            showToast('Failed to complete pipeline on backend.', 'error');
        } finally {
            setIsIngesting(false);
        }
    };

    // ─────────────────────────────────────────────────────────
    // Connector config form render
    // ─────────────────────────────────────────────────────────

    const renderConnectorConfig = () => {
        if (!selectedConnector) return null;
        return (
            <div className="animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem', maxWidth: '560px' }}>
                <Input
                    label="Pipeline Name"
                    placeholder="e.g., Daily Sales Sync"
                    value={connectorConfig.pipelineName || ''}
                    onChange={e => updateConfig('pipelineName', e.target.value)}
                />

                {selectedConnector === 'postgres' && (
                    <>
                        <h3 className="connector-config-title"><Database size={18} /> PostgreSQL Connection</h3>
                        <div className="connector-form-grid">
                            <Input label="Host *" placeholder="localhost or db.example.com" value={connectorConfig.pgHost || ''} onChange={e => updateConfig('pgHost', e.target.value)} />
                            <Input label="Port" placeholder="5432" value={connectorConfig.pgPort || ''} onChange={e => updateConfig('pgPort', e.target.value)} />
                        </div>
                        <Input label="Database Name *" placeholder="my_database" value={connectorConfig.pgDatabase || ''} onChange={e => updateConfig('pgDatabase', e.target.value)} />
                        <div className="connector-form-grid">
                            <Input label="Username *" placeholder="postgres" value={connectorConfig.pgUsername || ''} onChange={e => updateConfig('pgUsername', e.target.value)} />
                            <Input label="Password" type="password" placeholder="••••••••" value={connectorConfig.pgPassword || ''} onChange={e => updateConfig('pgPassword', e.target.value)} />
                        </div>
                        <Input
                            label="Table Name *"
                            placeholder={connectionMeta?.tables ? `e.g., ${connectionMeta.tables[0]}` : 'e.g., users'}
                            value={connectorConfig.pgTable || ''}
                            onChange={e => updateConfig('pgTable', e.target.value)}
                        />
                        {connectionMeta?.tables && (
                            <div className="connector-available-items">
                                <span className="connector-available-label">Available tables:</span>
                                <div className="connector-tags">
                                    {connectionMeta.tables.map((t: string) => (
                                        <button key={t} className="connector-tag" onClick={() => updateConfig('pgTable', t)} type="button">{t}</button>
                                    ))}
                                </div>
                            </div>
                        )}
                    </>
                )}

                {selectedConnector === 'mongo' && (
                    <>
                        <h3 className="connector-config-title"><FileJson size={18} /> MongoDB Connection</h3>
                        <Input label="Connection URI *" placeholder="mongodb://localhost:27017" value={connectorConfig.mongoUri || ''} onChange={e => updateConfig('mongoUri', e.target.value)} />
                        <Input label="Database Name" placeholder="my_database" value={connectorConfig.mongoDatabase || ''} onChange={e => updateConfig('mongoDatabase', e.target.value)} />
                        <Input
                            label="Collection Name *"
                            placeholder={connectionMeta?.collections ? `e.g., ${connectionMeta.collections[0]}` : 'e.g., users'}
                            value={connectorConfig.mongoCollection || ''}
                            onChange={e => updateConfig('mongoCollection', e.target.value)}
                        />
                        {connectionMeta?.collections && (
                            <div className="connector-available-items">
                                <span className="connector-available-label">Available collections:</span>
                                <div className="connector-tags">
                                    {connectionMeta.collections.map((c: string) => (
                                        <button key={c} className="connector-tag" onClick={() => updateConfig('mongoCollection', c)} type="button">{c}</button>
                                    ))}
                                </div>
                            </div>
                        )}
                    </>
                )}

                {selectedConnector === 'api' && (
                    <>
                        <h3 className="connector-config-title"><Share2 size={18} /> REST API Configuration</h3>
                        <div className="connector-form-grid connector-form-grid--api">
                            <div className="input-wrapper" style={{ maxWidth: '120px' }}>
                                <label className="input-label">Method</label>
                                <select className="input-field" value={connectorConfig.apiMethod || 'GET'} onChange={e => updateConfig('apiMethod', e.target.value)}>
                                    <option>GET</option>
                                    <option>POST</option>
                                </select>
                            </div>
                            <Input label="API URL *" placeholder="https://api.example.com/data" value={connectorConfig.apiUrl || ''} onChange={e => updateConfig('apiUrl', e.target.value)} style={{ flex: 1 }} />
                        </div>
                        <div className="input-wrapper">
                            <label className="input-label">Headers (JSON, optional)</label>
                            <textarea className="input-field connector-textarea" placeholder={'{\n  "Authorization": "Bearer YOUR_TOKEN"\n}'} value={connectorConfig.apiHeaders || ''} onChange={e => updateConfig('apiHeaders', e.target.value)} rows={3} />
                        </div>
                        <div className="connector-api-hint">
                            <AlertCircle size={14} />
                            <span>Tip: Try <code>https://jsonplaceholder.typicode.com/users</code> for a quick test.</span>
                        </div>
                    </>
                )}

                <div className="connector-test-row">
                    <Button variant="outline" onClick={handleTestConnection} disabled={connectionStatus === 'testing'}>
                        {connectionStatus === 'testing' ? <><Loader2 size={14} className="spinner" /> Testing...</> : 'Test Connection'}
                    </Button>
                    {connectionStatus === 'success' && (
                        <div className="connector-status connector-status--success"><CheckCircle size={16} /> {connectionMessage}</div>
                    )}
                    {connectionStatus === 'error' && (
                        <div className="connector-status connector-status--error"><XCircle size={16} /> {connectionMessage}</div>
                    )}
                </div>
            </div>
        );
    };

    // ─────────────────────────────────────────────────────────
    // Validation Status Styling
    // ─────────────────────────────────────────────────────────

    const getValidationStatus = (passRate: number) => {
        if (passRate === 100) return { color: 'var(--success-color)', label: 'All rows valid', Icon: ShieldCheck, cls: 'valid' };
        if (passRate >= 80) return { color: 'var(--warning-color)', label: 'Mostly valid — some issues', Icon: ShieldAlert, cls: 'warning' };
        return { color: 'var(--danger-color)', label: 'Significant validation failures', Icon: Shield, cls: 'error' };
    };

    // ─────────────────────────────────────────────────────────
    // RENDER
    // ─────────────────────────────────────────────────────────

    return (
        <div style={{ maxWidth: '1000px', margin: '0 auto', width: '100%' }}>
            <div style={{ marginBottom: '2rem' }}>
                <h1 style={{ fontSize: '1.875rem', marginBottom: '0.5rem' }}>Data Ingestion</h1>
                <p style={{ color: 'var(--text-secondary)' }}>Connect to a new data source or upload local files — schema inference, contract binding, and validation happen automatically.</p>
            </div>

            <Card>
                <CardHeader>New Ingestion Pipeline</CardHeader>
                <CardContent>
                    <Stepper steps={STEPS} currentStep={currentStep} />

                    <div style={{ marginTop: '2rem', minHeight: '300px' }}>

                        {/* ── Step 0: Select Source ── */}
                        {currentStep === 0 && (
                            <div className="animate-fade-in">
                                <div style={{ display: 'flex', gap: '1rem', marginBottom: '2rem' }}>
                                    <Button variant={sourceType === 'file' ? 'primary' : 'outline'} onClick={() => { setSourceType('file'); setSelectedConnector(null); }}>
                                        Upload File
                                    </Button>
                                    <Button variant={sourceType === 'connector' ? 'primary' : 'outline'} onClick={() => { setSourceType('connector'); setSelectedFile(null); }}>
                                        Third-Party Connector
                                    </Button>
                                </div>

                                {sourceType === 'file' && (
                                    <div>
                                        {selectedFile ? (
                                            <div className="file-drop-zone active" style={{ padding: '1.5rem', flexDirection: 'row', justifyContent: 'space-between' }}>
                                                <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                                                    <FileJson size={24} color="var(--primary-color)" />
                                                    <div style={{ textAlign: 'left' }}>
                                                        <div className="file-title">{selectedFile.name}</div>
                                                        <div className="file-subtitle">{(selectedFile.size / 1024).toFixed(2)} KB</div>
                                                    </div>
                                                </div>
                                                <Button variant="outline" onClick={() => setSelectedFile(null)}>Change File</Button>
                                            </div>
                                        ) : (
                                            <FileDropZone onFileSelect={setSelectedFile} />
                                        )}
                                    </div>
                                )}

                                {sourceType === 'connector' && (
                                    <div className="connector-grid">
                                        {CONNECTORS.map(connector => {
                                            const Icon = connector.icon;
                                            return (
                                                <div
                                                    key={connector.id}
                                                    className={`connector-card ${selectedConnector === connector.id ? 'selected' : ''}`}
                                                    onClick={() => {
                                                        setSelectedConnector(connector.id);
                                                        setConnectionStatus('idle');
                                                        setConnectionMessage('');
                                                        setConnectionMeta(null);
                                                    }}
                                                >
                                                    <div className="connector-icon-wrapper"><Icon size={24} color="var(--text-primary)" /></div>
                                                    <div>
                                                        <div className="connector-name">{connector.name}</div>
                                                        <div className="connector-type">{connector.type}</div>
                                                    </div>
                                                </div>
                                            );
                                        })}
                                    </div>
                                )}
                            </div>
                        )}

                        {/* ── Step 1: Configure ── */}
                        {currentStep === 1 && (
                            <div>
                                {sourceType === 'file' ? (
                                    <div className="animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem', maxWidth: '500px' }}>
                                        <h3 style={{ fontSize: '1.125rem' }}>Configuration Options</h3>
                                        <Input label="Pipeline Name" placeholder="e.g., Daily Sales Sync" />
                                        <Input label="Target Database Schema" placeholder="public" />
                                    </div>
                                ) : (
                                    renderConnectorConfig()
                                )}
                            </div>
                        )}

                        {/* ── Step 2: Preview Schema ── */}
                        {currentStep === 2 && (
                            <div className="animate-fade-in">
                                <h3 style={{ fontSize: '1.125rem', marginBottom: '0.5rem' }}>Inferred Schema Preview</h3>
                                <p style={{ color: 'var(--text-secondary)', marginBottom: '1rem', fontSize: '0.875rem' }}>
                                    {sourceType === 'connector'
                                        ? `Pulled ${pulledRowCount} rows from ${selectedConnector?.toUpperCase()}.`
                                        : 'AI has detected the following schema from your file.'}
                                </p>

                                {/* Auto Contract Badge */}
                                {pipelineResult && (
                                    <div className={`ingestion-contract-badge ${pipelineResult.contract.autoCreated ? 'auto-created' : 'matched'}`}>
                                        {pipelineResult.contract.autoCreated ? (
                                            <><Sparkles size={14} /> Auto-generated contract: <strong>{pipelineResult.contract.name}</strong> v{pipelineResult.contract.version}</>
                                        ) : (
                                            <><CheckCircle2 size={14} /> Matched existing contract: <strong>{pipelineResult.contract.name}</strong> v{pipelineResult.contract.version}</>
                                        )}
                                    </div>
                                )}

                                {/* Data Preview for connector pulls */}
                                {sourceType === 'connector' && pulledPreview.length > 0 && (
                                    <div className="connector-data-preview" style={{ marginTop: '1rem' }}>
                                        <h4 style={{ fontSize: '0.875rem', fontWeight: 600, marginBottom: '0.75rem', color: 'var(--text-secondary)' }}>
                                            Data Preview (first {Math.min(pulledPreview.length, 5)} rows)
                                        </h4>
                                        <div style={{ overflowX: 'auto' }}>
                                            <table className="connector-preview-table">
                                                <thead>
                                                    <tr>{Object.keys(pulledPreview[0]).map(key => <th key={key}>{key}</th>)}</tr>
                                                </thead>
                                                <tbody>
                                                    {pulledPreview.slice(0, 5).map((row, rowIdx) => (
                                                        <tr key={rowIdx}>
                                                            {Object.values(row).map((val, colIdx) => (
                                                                <td key={colIdx}>{typeof val === 'object' ? JSON.stringify(val) : String(val ?? '')}</td>
                                                            ))}
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    </div>
                                )}

                                {/* Schema Table */}
                                <div style={{ display: 'grid', gridTemplateColumns: 'minmax(200px,1fr) 200px', gap: '1rem', borderBottom: '1px solid var(--border-color)', paddingBottom: '0.5rem', fontWeight: 600, marginTop: '1.5rem' }}>
                                    <div>Column Name</div>
                                    <div>Inferred Type</div>
                                </div>
                                {inferredSchema.length > 0 ? inferredSchema.map(col => (
                                    <div key={col.name} style={{ display: 'grid', gridTemplateColumns: 'minmax(200px,1fr) 200px', gap: '1rem', padding: '1rem 0', borderBottom: '1px solid var(--border-color)', alignItems: 'center' }}>
                                        <div style={{ fontFamily: 'monospace' }}>
                                            {col.name}
                                            {col.description && <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>{col.description}</div>}
                                        </div>
                                        <select className="input-field" defaultValue={col.type}>
                                            {['String', 'Integer', 'Float', 'Date', 'Time', 'UUID', 'Boolean', 'Object', 'Array'].map(t => <option key={t}>{t}</option>)}
                                        </select>
                                    </div>
                                )) : (
                                    <div style={{ padding: '2rem', textAlign: 'center', color: 'var(--text-secondary)' }}>No schema found.</div>
                                )}
                            </div>
                        )}

                        {/* ── Step 3: Validation Preview ── */}
                        {currentStep === 3 && pipelineResult && (
                            <div className="animate-fade-in">
                                <h3 style={{ fontSize: '1.125rem', marginBottom: '0.25rem' }}>Validation Preview</h3>
                                <p style={{ color: 'var(--text-secondary)', fontSize: '0.875rem', marginBottom: '1.5rem' }}>
                                    Your data was validated against <strong>{pipelineResult.contract.name}</strong>. Review the results and select an enforcement mode before ingesting.
                                </p>

                                {/* Metrics Row */}
                                <div className="validation-metrics-grid">
                                    <div className="validation-metric-card">
                                        <div className="validation-metric-value">{pipelineResult.validationReport.totalRows}</div>
                                        <div className="validation-metric-label">Total Rows</div>
                                    </div>
                                    <div className="validation-metric-card valid">
                                        <div className="validation-metric-value" style={{ color: 'var(--success-color)' }}>{pipelineResult.validationReport.validRows}</div>
                                        <div className="validation-metric-label">Valid Rows</div>
                                    </div>
                                    <div className="validation-metric-card" style={{ borderColor: pipelineResult.validationReport.invalidRows > 0 ? 'var(--danger-color)' : undefined }}>
                                        <div className="validation-metric-value" style={{ color: pipelineResult.validationReport.invalidRows > 0 ? 'var(--danger-color)' : 'var(--success-color)' }}>
                                            {pipelineResult.validationReport.invalidRows}
                                        </div>
                                        <div className="validation-metric-label">Invalid Rows</div>
                                    </div>
                                    <div className="validation-metric-card">
                                        <div className="validation-metric-value">{pipelineResult.validationReport.passRate}%</div>
                                        <div className="validation-metric-label">Pass Rate</div>
                                    </div>
                                    <div className="validation-metric-card">
                                        <div className="validation-metric-value">{pipelineResult.validationReport.overallScore}</div>
                                        <div className="validation-metric-label">Quality Score</div>
                                    </div>
                                </div>

                                {/* Quality dimensions sub-metrics */}
                                <div className="validation-dimensions">
                                    {[
                                        { label: 'Completeness', value: pipelineResult.validationReport.completeness },
                                        { label: 'Validity', value: pipelineResult.validationReport.validity },
                                        { label: 'Uniqueness', value: pipelineResult.validationReport.uniqueness },
                                    ].map(({ label, value }) => (
                                        <div key={label} className="validation-dimension">
                                            <div className="validation-dimension-label">{label}</div>
                                            <div className="validation-dimension-bar">
                                                <div
                                                    className="validation-dimension-fill"
                                                    style={{
                                                        width: `${value}%`,
                                                        background: value >= 90 ? 'var(--success-color)' : value >= 70 ? 'var(--warning-color)' : 'var(--danger-color)',
                                                    }}
                                                />
                                            </div>
                                            <div className="validation-dimension-value">{value}%</div>
                                        </div>
                                    ))}
                                </div>

                                {/* Status Indicator */}
                                {(() => {
                                    const s = getValidationStatus(pipelineResult.validationReport.passRate);
                                    return (
                                        <div className={`validation-status-banner validation-status-banner--${s.cls}`}>
                                            <s.Icon size={18} />
                                            <span>{s.label}</span>
                                        </div>
                                    );
                                })()}

                                {/* Enforcement Mode Selector */}
                                <div className="enforcement-selector">
                                    <div className="enforcement-selector-label">
                                        <Shield size={16} />
                                        <strong>Enforcement Mode</strong>
                                        <span style={{ color: 'var(--text-secondary)', fontSize: '0.8125rem', marginLeft: '0.5rem' }}>
                                            — Controls how validation failures are handled
                                        </span>
                                    </div>
                                    <div className="enforcement-options">
                                        {([
                                            { value: 'strict', label: 'Strict', desc: 'Reject invalid rows — only valid rows stored', icon: '🔒' },
                                            { value: 'warning', label: 'Warning', desc: 'Store all rows, attach validation report', icon: '⚠️' },
                                            { value: 'monitor', label: 'Monitor', desc: 'Store all rows, log issues silently', icon: '👁️' },
                                        ] as const).map(opt => (
                                            <button
                                                key={opt.value}
                                                className={`enforcement-option ${enforcementMode === opt.value ? 'selected' : ''}`}
                                                onClick={() => setEnforcementMode(opt.value)}
                                                type="button"
                                            >
                                                <span className="enforcement-option-icon">{opt.icon}</span>
                                                <div className="enforcement-option-content">
                                                    <div className="enforcement-option-label">{opt.label}</div>
                                                    <div className="enforcement-option-desc">{opt.desc}</div>
                                                </div>
                                            </button>
                                        ))}
                                    </div>
                                </div>

                                {/* Sample Errors */}
                                {pipelineResult.validationReport.errors.length > 0 && (
                                    <div className="validation-errors-section">
                                        <div className="validation-errors-header">
                                            <AlertTriangle size={16} style={{ color: 'var(--warning-color)' }} />
                                            <span>Validation Issues ({pipelineResult.validationReport.invalidRows} affected rows)</span>
                                        </div>
                                        <div className="validation-errors-table">
                                            <div className="validation-errors-table-head">
                                                <span>Row</span><span>Field</span><span>Rule</span><span>Details</span>
                                            </div>
                                            {(showAllErrors
                                                ? pipelineResult.validationReport.errors
                                                : pipelineResult.validationReport.errors.slice(0, 8)
                                            ).map((err, idx) => (
                                                <div key={idx} className={`validation-error-row severity-${err.severity}`}>
                                                    <span>#{err.row + 1}</span>
                                                    <span style={{ fontFamily: 'monospace' }}>{err.field}</span>
                                                    <span className="validation-error-rule">{err.rule}</span>
                                                    <span style={{ color: 'var(--text-secondary)', fontSize: '0.75rem' }}>
                                                        Expected: {err.expected} / Got: {err.actual}
                                                    </span>
                                                </div>
                                            ))}
                                        </div>
                                        {pipelineResult.validationReport.errors.length > 8 && (
                                            <button
                                                className="validation-show-more"
                                                onClick={() => setShowAllErrors(prev => !prev)}
                                                type="button"
                                            >
                                                <ChevronDown size={14} />
                                                {showAllErrors ? 'Show less' : `Show all ${pipelineResult.validationReport.errors.length} issues`}
                                            </button>
                                        )}
                                    </div>
                                )}
                            </div>
                        )}

                        {/* ── Step 3: Validation Preview (no result yet — fallback) ── */}
                        {currentStep === 3 && !pipelineResult && (
                            <div className="animate-fade-in" style={{ textAlign: 'center', padding: '3rem' }}>
                                <AlertCircle size={40} style={{ color: 'var(--warning-color)', marginBottom: '1rem' }} />
                                <p style={{ color: 'var(--text-secondary)' }}>No validation result available. Please go back and re-upload your data.</p>
                            </div>
                        )}

                        {/* ── Step 4: Ingest (Confirmation) ── */}
                        {currentStep === 4 && (
                            <div className="animate-fade-in" style={{ display: 'flex', flexDirection: 'column', gap: '1.5rem', maxWidth: '560px' }}>
                                <h3 style={{ fontSize: '1.125rem' }}>Ready to Ingest</h3>
                                <p style={{ color: 'var(--text-secondary)', fontSize: '0.875rem' }}>
                                    Review your pipeline configuration and click "Finish & Ingest" to complete the process.
                                </p>

                                {ingestionDone ? (
                                    <div className="ingestion-success-banner">
                                        <CheckCircle2 size={32} style={{ color: 'var(--success-color)' }} />
                                        <div>
                                            <div style={{ fontWeight: 600, marginBottom: '0.25rem' }}>Ingestion Complete!</div>
                                            <div style={{ fontSize: '0.875rem', color: 'var(--text-secondary)' }}>Your pipeline has been saved and a workflow task created.</div>
                                        </div>
                                    </div>
                                ) : (
                                    <div className="ingestion-summary-card">
                                        <div className="ingestion-summary-row">
                                            <span>Dataset</span>
                                            <strong>{pipelineResult?.dataset.name || selectedFile?.name || connectorConfig.pipelineName}</strong>
                                        </div>
                                        {pipelineResult && (
                                            <>
                                                <div className="ingestion-summary-row">
                                                    <span>Contract</span>
                                                    <strong>{pipelineResult.contract.name} v{pipelineResult.contract.version}</strong>
                                                </div>
                                                <div className="ingestion-summary-row">
                                                    <span>Rows Stored</span>
                                                    <strong>{pipelineResult.dataset.storedRows} / {pipelineResult.validationReport.totalRows}</strong>
                                                </div>
                                                <div className="ingestion-summary-row">
                                                    <span>Quality Score</span>
                                                    <strong style={{ color: pipelineResult.validationReport.overallScore >= 80 ? 'var(--success-color)' : 'var(--warning-color)' }}>
                                                        {pipelineResult.validationReport.overallScore}/100
                                                    </strong>
                                                </div>
                                                <div className="ingestion-summary-row">
                                                    <span>Enforcement Mode</span>
                                                    <strong style={{ textTransform: 'capitalize' }}>{pipelineResult.enforcementMode}</strong>
                                                </div>
                                                <div className="ingestion-summary-row">
                                                    <span>Status</span>
                                                    <strong style={{ color: pipelineResult.status === 'VALIDATED' ? 'var(--success-color)' : 'var(--warning-color)' }}>
                                                        {pipelineResult.status.replace(/_/g, ' ')}
                                                    </strong>
                                                </div>
                                            </>
                                        )}
                                    </div>
                                )}
                            </div>
                        )}

                    </div>
                </CardContent>
                <CardFooter style={{ display: 'flex', justifyContent: 'space-between' }}>
                    <Button variant="secondary" onClick={handleBack} disabled={currentStep === 0 || ingestionDone}>Back</Button>
                    <Button
                        variant="primary"
                        onClick={handleNext}
                        disabled={isInferring || isPulling || isIngesting || ingestionDone}
                    >
                        {isInferring || isPulling ? (
                            <><Loader2 size={14} className="spinner" /> Processing...</>
                        ) : isIngesting ? (
                            <><Loader2 size={14} className="spinner" /> Ingesting...</>
                        ) : currentStep === STEPS.length - 1 ? (
                            'Finish & Ingest'
                        ) : (
                            'Continue'
                        )}
                    </Button>
                </CardFooter>
            </Card>
        </div>
    );
}
