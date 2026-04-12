import { useEffect, useMemo, useRef, useState } from 'react';
import {
  Area,
  AreaChart,
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  ComposedChart,
  Legend,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip as RechartsTooltip,
  XAxis,
  YAxis,
} from 'recharts';
import { Activity, ChartArea, LayoutDashboard, Loader2, Menu, Stethoscope, Wallet, X } from 'lucide-react';

const VIEW_OPTIONS = [
  { id: 'overview', label: 'Overview', icon: LayoutDashboard },
  { id: 'growth', label: 'Growth & Onboarding', icon: Activity },
  { id: 'usage', label: 'Usage Scenarios', icon: ChartArea },
  { id: 'cost', label: 'Cost & Token Control', icon: Wallet },
  { id: 'quality', label: 'Quality & Diagnostics', icon: Stethoscope },
] as const;

const PRESET_OPTIONS = [
  { label: 'This Month', value: 'this_month' },
  { label: 'Last 30 Days', value: 'last_30_days' },
  { label: 'This Quarter', value: 'this_quarter' },
  { label: 'YTD', value: 'ytd' },
] as const;

type DashboardView = (typeof VIEW_OPTIONS)[number]['id'];
type PeriodPreset = (typeof PRESET_OPTIONS)[number]['value'];
const DIAGNOSTIC_CATEGORIES = ['Hallucination', 'OutdatedInfo', 'Tone', 'InstructionsUnfollowed'] as const;

type CacheStore = {
  kpi: Map<PeriodPreset, any>;
  growth: Map<PeriodPreset, any>;
  sources: Map<PeriodPreset, any>;
  sourceMix: Map<PeriodPreset, any>;
  selfColleague: Map<PeriodPreset, any>;
  pareto: Map<PeriodPreset, any>;
  efficiency: Map<PeriodPreset, any>;
  qualityOverview: Map<PeriodPreset, any>;
  qualityDepartmentRisk: Map<string, any>;
};

function parseViewFromUrl(): DashboardView {
  const view = new URLSearchParams(window.location.search).get('view');
  return view === 'growth' || view === 'usage' || view === 'cost' || view === 'quality' ? view : 'overview';
}

function formatNumericDelta(delta: number | null): string {
  if (delta === null) return 'vs previous period N/A';
  const direction = delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat';
  return `vs previous period ${direction} ${Math.abs(delta).toFixed(1)}%`;
}

function formatRatioDelta(delta: number | null): string {
  if (delta === null) return 'vs previous period N/A';
  const direction = delta > 0 ? 'up' : delta < 0 ? 'down' : 'flat';
  return `vs previous period ${direction} ${Math.abs(delta).toFixed(1)}pp`;
}

function formatCurrency(value: number | null, currency = 'USD'): string {
  if (value === null) return 'N/A';
  return new Intl.NumberFormat('en-US', { style: 'currency', currency, minimumFractionDigits: 2, maximumFractionDigits: 4 }).format(value);
}

function formatBucketLabel(value: string): string {
  const date = new Date(value);
  return `${date.getMonth() + 1}/${date.getDate()}`;
}

function formatDiagnosticCategory(value: string): string {
  if (value === 'OutdatedInfo') return 'Outdated Info';
  if (value === 'InstructionsUnfollowed') return 'Instructions Unfollowed';
  return value;
}

async function fetchJson<T>(url: string): Promise<T> {
  const response = await fetch(url);
  if (!response.ok) throw new Error(`Request failed (${response.status})`);
  return (await response.json()) as T;
}

export default function App() {
  const [preset, setPreset] = useState<PeriodPreset>('this_month');
  const [view, setView] = useState<DashboardView>(() => parseViewFromUrl());
  const [drawerOpen, setDrawerOpen] = useState(false);
  const [qualitySelectedDepartment, setQualitySelectedDepartment] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [, rerender] = useState(0);

  const cacheRef = useRef<CacheStore>({
    kpi: new Map(),
    growth: new Map(),
    sources: new Map(),
    sourceMix: new Map(),
    selfColleague: new Map(),
    pareto: new Map(),
    efficiency: new Map(),
    qualityOverview: new Map(),
    qualityDepartmentRisk: new Map(),
  });
  const inFlightRef = useRef(new Map<string, Promise<any>>());

  const loadCached = async <T,>(
    key: Exclude<keyof CacheStore, 'qualityDepartmentRisk'>,
    p: PeriodPreset,
    fetcher: () => Promise<T>,
  ): Promise<T> => {
    const cache = cacheRef.current[key] as Map<PeriodPreset, T>;
    const existing = cache.get(p);
    if (existing !== undefined) return existing;

    const flightKey = `${String(key)}:${p}`;
    const inFlight = inFlightRef.current.get(flightKey) as Promise<T> | undefined;
    if (inFlight) return inFlight;

    const request = fetcher().then((data) => {
      cache.set(p, data);
      rerender((v) => v + 1);
      return data;
    }).finally(() => inFlightRef.current.delete(flightKey));

    inFlightRef.current.set(flightKey, request);
    return request;
  };

  const ensureKpi = (p: PeriodPreset) => loadCached('kpi', p, () => fetchJson(`http://localhost:8000/api/metrics/kpi?preset=${p}`));
  const ensureGrowth = (p: PeriodPreset) => loadCached('growth', p, () => fetchJson(`http://localhost:8000/api/modules/growth/overview?preset=${p}`));
  const ensureSources = (p: PeriodPreset) => loadCached('sources', p, () => fetchJson(`http://localhost:8000/api/metrics/sources?preset=${p}`));
  const ensureSourceMix = (p: PeriodPreset) => loadCached('sourceMix', p, () => fetchJson(`http://localhost:8000/api/modules/usage/source-mix?preset=${p}`));
  const ensureSelfColleague = (p: PeriodPreset) => loadCached('selfColleague', p, () => fetchJson(`http://localhost:8000/api/modules/usage/self-colleague-share?preset=${p}`));
  const ensurePareto = (p: PeriodPreset) => loadCached('pareto', p, () => fetchJson(`http://localhost:8000/api/modules/cost/pareto?preset=${p}`));
  const ensureEfficiency = (p: PeriodPreset) => loadCached('efficiency', p, () => fetchJson(`http://localhost:8000/api/modules/cost/efficiency?preset=${p}`));
  const ensureQualityOverview = (p: PeriodPreset) => loadCached('qualityOverview', p, () => fetchJson(`http://localhost:8000/api/modules/quality/overview?preset=${p}`));

  const ensureQualityDepartmentRisk = async (p: PeriodPreset, department?: string): Promise<any> => {
    const depKey = department ?? '__default';
    const cacheKey = `${p}:${depKey}`;
    const cache = cacheRef.current.qualityDepartmentRisk;
    const existing = cache.get(cacheKey);
    if (existing !== undefined) return existing;

    const flightKey = `qualityDepartmentRisk:${cacheKey}`;
    const inFlight = inFlightRef.current.get(flightKey);
    if (inFlight) return inFlight;

    const url = new URL('http://localhost:8000/api/modules/quality/department-risk');
    url.searchParams.set('preset', p);
    if (department) url.searchParams.set('department', department);

    const request = fetchJson(url.toString())
      .then((data) => {
        cache.set(cacheKey, data);
        rerender((v) => v + 1);
        return data;
      })
      .finally(() => inFlightRef.current.delete(flightKey));

    inFlightRef.current.set(flightKey, request);
    return request;
  };

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    params.set('view', view);
    window.history.replaceState(null, '', `${window.location.pathname}?${params.toString()}`);
  }, [view]);

  useEffect(() => {
    const onPop = () => setView(parseViewFromUrl());
    window.addEventListener('popstate', onPop);
    return () => window.removeEventListener('popstate', onPop);
  }, []);

  useEffect(() => {
    setQualitySelectedDepartment(null);
  }, [preset]);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        if (view === 'overview') {
          await Promise.all([
            ensureKpi(preset),
            ensureGrowth(preset),
            ensureSources(preset),
            ensureSelfColleague(preset),
            ensurePareto(preset),
            ensureEfficiency(preset),
            ensureQualityOverview(preset),
          ]);
          void ensureSourceMix(preset);
          void ensureQualityDepartmentRisk(preset);
        } else if (view === 'growth') {
          await ensureGrowth(preset);
        } else if (view === 'usage') {
          await Promise.all([ensureSourceMix(preset), ensureSelfColleague(preset)]);
        } else if (view === 'cost') {
          await Promise.all([ensurePareto(preset), ensureEfficiency(preset)]);
        } else {
          const [overviewData, defaultRisk] = await Promise.all([
            ensureQualityOverview(preset),
            ensureQualityDepartmentRisk(preset),
          ]);

          const nextDepartment = qualitySelectedDepartment ?? defaultRisk?.selected_department ?? null;
          if (nextDepartment && nextDepartment !== qualitySelectedDepartment && !cancelled) {
            setQualitySelectedDepartment(nextDepartment);
          }
          if (nextDepartment) {
            await ensureQualityDepartmentRisk(preset, nextDepartment);
          }

          if (!overviewData || !defaultRisk) {
            throw new Error('Quality data unavailable');
          }
        }
      } catch (e) {
        if (!cancelled) setError(e instanceof Error ? e.message : 'Loading failed');
      } finally {
        if (!cancelled) setLoading(false);
      }
    };

    load();
    return () => { cancelled = true; };
  }, [view, preset, qualitySelectedDepartment]);

  const kpi = cacheRef.current.kpi.get(preset);
  const growth = cacheRef.current.growth.get(preset);
  const sources = cacheRef.current.sources.get(preset) ?? [];
  const sourceMix = cacheRef.current.sourceMix.get(preset);
  const selfColleague = cacheRef.current.selfColleague.get(preset);
  const pareto = cacheRef.current.pareto.get(preset);
  const efficiency = cacheRef.current.efficiency.get(preset);
  const qualityOverview = cacheRef.current.qualityOverview.get(preset);
  const qualityRiskDefault = cacheRef.current.qualityDepartmentRisk.get(`${preset}:__default`);
  const qualityRiskSelected = qualitySelectedDepartment
    ? cacheRef.current.qualityDepartmentRisk.get(`${preset}:${qualitySelectedDepartment}`) ?? qualityRiskDefault
    : qualityRiskDefault;

  const latestShare = useMemo(() => {
    if (!selfColleague?.series?.length) return { self: 0, colleague: 0 };
    const points = selfColleague.series;
    const latest = points[points.length - 1];
    return { self: latest.self_share_pct ?? 0, colleague: latest.colleague_share_pct ?? 0 };
  }, [selfColleague]);

  const sourceSummary = useMemo(() => {
    const total = sources.reduce((sum: number, s: any) => sum + s.count, 0);
    return sources.map((s: any) => ({ ...s, share: total > 0 ? (s.count / total) * 100 : 0 }));
  }, [sources]);

  const top20Share = useMemo(() => {
    if (!pareto?.series?.length) return 0;
    const cutoff = Math.max(1, Math.ceil(pareto.series.length * 0.2));
    return pareto.series.slice(0, cutoff).reduce((sum: number, p: any) => sum + p.cost_share_pct, 0);
  }, [pareto]);

  const topQualityDefect = useMemo(() => {
    if (!qualityOverview?.defect_breakdown?.length) return null;

    const totals: Record<string, number> = {
      Hallucination: 0,
      OutdatedInfo: 0,
      Tone: 0,
      InstructionsUnfollowed: 0,
    };

    for (const point of qualityOverview.defect_breakdown) {
      for (const category of DIAGNOSTIC_CATEGORIES) {
        totals[category] += point[category] ?? 0;
      }
    }

    const [top] = Object.entries(totals).sort((a, b) => b[1] - a[1]);
    if (!top || top[1] <= 0) return null;
    return { category: top[0], count: top[1] };
  }, [qualityOverview]);

  const title = VIEW_OPTIONS.find((v) => v.id === view)?.label ?? 'Overview';

  const navItemClass = (id: DashboardView) => `w-full flex items-center gap-3 rounded-lg px-3 py-2 text-sm transition ${view === id ? 'bg-slate-900 text-white' : 'text-slate-600 hover:bg-slate-100 hover:text-slate-900'}`;
  const handleDepartmentBarClick = (item: any) => {
    const department = item?.department ?? item?.payload?.department ?? item?.activePayload?.[0]?.payload?.department;
    if (!department) return;
    setQualitySelectedDepartment(department);
    void ensureQualityDepartmentRisk(preset, department);
  };

  return (
    <div className="min-h-screen bg-slate-50 text-slate-800">
      <header className="sticky top-0 z-30 border-b border-slate-200 bg-slate-50/95 backdrop-blur">
        <div className="max-w-7xl mx-auto p-4 md:p-6 flex flex-col gap-4">
          <div className="flex items-center justify-between">
            <div>
              <h1 className="text-2xl md:text-3xl font-bold text-slate-900 tracking-tight">AI Twin Analytics</h1>
              <p className="text-slate-500 text-sm mt-1">Overview + module navigation for enterprise diagnosis</p>
            </div>
            <button className="md:hidden rounded-lg border border-slate-200 bg-white p-2" onClick={() => setDrawerOpen(true)}>
              <Menu className="w-5 h-5" />
            </button>
          </div>

          <div className="w-full md:w-64">
            <label htmlFor="global-period" className="block text-xs font-semibold text-slate-500 mb-2 uppercase tracking-wide">Global Date Picker</label>
            <select id="global-period" value={preset} onChange={(e) => setPreset(e.target.value as PeriodPreset)} className="w-full rounded-lg border border-slate-200 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm">
              {PRESET_OPTIONS.map((option) => <option key={option.value} value={option.value}>{option.label}</option>)}
            </select>
          </div>
        </div>
      </header>

      {drawerOpen && (
        <div className="fixed inset-0 z-40 md:hidden">
          <button className="absolute inset-0 bg-black/30" onClick={() => setDrawerOpen(false)} />
          <div className="absolute left-0 top-0 h-full w-72 bg-white shadow-xl p-4">
            <div className="flex items-center justify-between mb-4"><h2 className="font-semibold">Modules</h2><button onClick={() => setDrawerOpen(false)}><X className="w-5 h-5" /></button></div>
            <nav className="space-y-1">
              {VIEW_OPTIONS.map((item) => {
                const Icon = item.icon;
                return <button key={item.id} className={navItemClass(item.id)} onClick={() => { setView(item.id); setDrawerOpen(false); }}><Icon className="w-4 h-4" />{item.label}</button>;
              })}
            </nav>
          </div>
        </div>
      )}

      <main className="max-w-7xl mx-auto p-4 md:p-6">
        <div className="flex gap-6">
          <aside className="hidden md:block w-64 shrink-0">
            <div className="bg-white border border-slate-100 rounded-xl p-3 shadow-sm sticky top-[176px]">
              <p className="text-xs uppercase tracking-wide text-slate-500 px-2 pb-2">Modules</p>
              <nav className="space-y-1">
                {VIEW_OPTIONS.map((item) => {
                  const Icon = item.icon;
                  return <button key={item.id} className={navItemClass(item.id)} onClick={() => setView(item.id)}><Icon className="w-4 h-4" />{item.label}</button>;
                })}
              </nav>
            </div>
          </aside>

          <section className="flex-1 space-y-4 min-w-0">
            <div className="flex items-center justify-between"><h2 className="text-xl font-semibold text-slate-900">{title}</h2>{loading && <span className="text-xs text-slate-500">Updating...</span>}</div>
            {error && <div className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">{error}</div>}

            {loading && view === 'overview' && !kpi && <div className="bg-white rounded-xl p-10 border border-slate-100 flex items-center justify-center"><Loader2 className="w-6 h-6 animate-spin text-blue-500" /></div>}

            {view === 'overview' && kpi && growth && selfColleague && pareto && efficiency && qualityOverview && (
              <div className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
                  <div className="bg-white p-4 rounded-xl border border-slate-100"><p className="text-xs text-slate-500">Total Queries</p><p className="text-2xl font-bold mt-1">{kpi.total_interactions.toLocaleString()}</p><p className="text-xs text-slate-500 mt-1">{formatNumericDelta(kpi.delta.total_interactions.delta_pct)}</p></div>
                  <div className="bg-white p-4 rounded-xl border border-slate-100"><p className="text-xs text-slate-500">Active Twins</p><p className="text-2xl font-bold mt-1">{kpi.active_twins}</p><p className="text-xs text-slate-500 mt-1">{formatNumericDelta(kpi.delta.active_twins.delta_pct)}</p></div>
                  <div className="bg-white p-4 rounded-xl border border-slate-100"><p className="text-xs text-slate-500">Avg Latency</p><p className="text-2xl font-bold mt-1">{kpi.avg_latency_ms} ms</p><p className="text-xs text-slate-500 mt-1">{formatNumericDelta(kpi.delta.avg_latency_ms.delta_pct)}</p></div>
                  <div className="bg-white p-4 rounded-xl border border-slate-100"><p className="text-xs text-slate-500">Tokens</p><p className="text-2xl font-bold mt-1">{(kpi.total_tokens / 1000).toFixed(1)}k</p><p className="text-xs text-slate-500 mt-1">{formatNumericDelta(kpi.delta.total_tokens.delta_pct)}</p></div>
                  <div className="bg-white p-4 rounded-xl border border-slate-100"><p className="text-xs text-slate-500">Public Twins %</p><p className="text-2xl font-bold mt-1">{kpi.public_twin_ratio_pct.toFixed(1)}%</p><p className="text-xs text-slate-500 mt-1">{formatRatioDelta(kpi.delta.public_twin_ratio_pct.delta_pp)}</p></div>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-4 gap-6">
                  <div className="bg-white p-5 rounded-xl border border-slate-100">
                    <div className="flex items-center justify-between"><h3 className="font-semibold">Growth Snapshot</h3><button className="text-xs text-blue-600" onClick={() => setView('growth')}>Open</button></div>
                    <p className="text-sm mt-3">Creation Rate: <span className="font-semibold">{growth.summary.twin_creation_rate.current?.toFixed(1) ?? '0.0'}%</span></p>
                    <p className="text-sm">Public Twin Rate: <span className="font-semibold">{growth.summary.public_twin_rate.current?.toFixed(1) ?? '0.0'}%</span></p>
                    <div className="h-24 mt-3"><ResponsiveContainer width="100%" height="100%"><LineChart data={growth.series}><Line dataKey="registered_users_cum" stroke="#1d4ed8" dot={false} /><Line dataKey="created_twins_cum" stroke="#059669" dot={false} /><Line dataKey="public_twins_cum" stroke="#f59e0b" dot={false} /></LineChart></ResponsiveContainer></div>
                  </div>

                  <div className="bg-white p-5 rounded-xl border border-slate-100">
                    <div className="flex items-center justify-between"><h3 className="font-semibold">Usage Snapshot</h3><button className="text-xs text-blue-600" onClick={() => setView('usage')}>Open</button></div>
                    <p className="text-sm mt-3">Self {latestShare.self.toFixed(1)}% / Colleague {latestShare.colleague.toFixed(1)}%</p>
                    <div className="mt-3 space-y-2">
                      {sourceSummary.map((item: any) => (
                        <div key={item.source}>
                          <div className="flex justify-between text-xs text-slate-500"><span>{item.source}</span><span>{item.share.toFixed(1)}%</span></div>
                          <div className="h-2 bg-slate-100 rounded overflow-hidden"><div className="h-full bg-blue-500" style={{ width: `${item.share}%` }} /></div>
                        </div>
                      ))}
                    </div>
                  </div>

                  <div className="bg-white p-5 rounded-xl border border-slate-100">
                    <div className="flex items-center justify-between"><h3 className="font-semibold">Cost Snapshot</h3><button className="text-xs text-blue-600" onClick={() => setView('cost')}>Open</button></div>
                    <p className="text-sm mt-3">Avg Cost / Colleague Solution</p>
                    <p className="text-xl font-semibold">{formatCurrency(efficiency.avg_cost_per_colleague_solution, efficiency.currency)}</p>
                    <p className="text-sm mt-2">Top-20% Cost Share: <span className="font-semibold">{top20Share.toFixed(1)}%</span></p>
                    <div className="h-2 bg-slate-100 rounded mt-2 overflow-hidden"><div className="h-full bg-rose-500" style={{ width: `${Math.min(top20Share, 100)}%` }} /></div>
                  </div>
                  <div className="bg-white p-5 rounded-xl border border-slate-100">
                    <div className="flex items-center justify-between"><h3 className="font-semibold">Quality Snapshot</h3><button className="text-xs text-blue-600" onClick={() => setView('quality')}>Open</button></div>
                    <p className="text-sm mt-3">Thumb-down Rate</p>
                    <p className="text-xl font-semibold">{(qualityOverview.summary.current_thumb_down_rate_pct ?? 0).toFixed(1)}%</p>
                    <p className="text-xs text-slate-500 mt-1">{formatRatioDelta(qualityOverview.summary.thumb_down_rate_delta_pp ?? null)}</p>
                    <p className="text-sm mt-3">Top Defect</p>
                    <p className="text-sm font-semibold">{topQualityDefect ? `${topQualityDefect.category} (${topQualityDefect.count})` : 'No tagged defects'}</p>
                  </div>
                </div>
              </div>
            )}

            {view === 'growth' && growth && (
              <section className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <div className="bg-white p-6 rounded-xl border border-slate-100"><p className="text-sm text-slate-500">Twin Creation Rate</p><h3 className="text-3xl font-bold mt-1">{growth.summary.twin_creation_rate.current?.toFixed(1) ?? '0.0'}%</h3><p className="text-xs text-slate-500 mt-2">{formatRatioDelta(growth.summary.twin_creation_rate.delta_pp ?? null)}</p></div>
                  <div className="bg-white p-6 rounded-xl border border-slate-100"><p className="text-sm text-slate-500">Public Twin Rate</p><h3 className="text-3xl font-bold mt-1">{growth.summary.public_twin_rate.current?.toFixed(1) ?? '0.0'}%</h3><p className="text-xs text-slate-500 mt-2">{formatRatioDelta(growth.summary.public_twin_rate.delta_pp ?? null)}</p></div>
                </div>
                <div className="bg-white p-6 rounded-xl border border-slate-100"><h3 className="text-lg font-semibold mb-6">Cumulative Growth (Weekly)</h3><div className="h-96"><ResponsiveContainer width="100%" height="100%"><LineChart data={growth.series}><CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" /><XAxis dataKey="bucket_start" tickFormatter={formatBucketLabel} /><YAxis /><RechartsTooltip /><Legend /><Line dataKey="registered_users_cum" stroke="#1d4ed8" dot={false} /><Line dataKey="created_twins_cum" stroke="#059669" dot={false} /><Line dataKey="public_twins_cum" stroke="#f59e0b" dot={false} /></LineChart></ResponsiveContainer></div></div>
              </section>
            )}

            {view === 'usage' && sourceMix && selfColleague && (
              <section className="space-y-6">
                <div className="bg-white p-6 rounded-xl border border-slate-100"><h3 className="text-lg font-semibold mb-2">Source Mix Trend ({sourceMix.granularity})</h3><p className="text-sm text-slate-500 mb-6">Stacked area across Slack DM, Slack Channel, and Web.</p><div className="h-96"><ResponsiveContainer width="100%" height="100%"><AreaChart data={sourceMix.series}><CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" /><XAxis dataKey="bucket_start" tickFormatter={formatBucketLabel} /><YAxis /><RechartsTooltip /><Legend /><Area dataKey="slack_dm" stackId="1" stroke="#3b82f6" fill="#3b82f6" fillOpacity={0.8} /><Area dataKey="slack_channel" stackId="1" stroke="#10b981" fill="#10b981" fillOpacity={0.8} /><Area dataKey="web_app" stackId="1" stroke="#f59e0b" fill="#f59e0b" fillOpacity={0.8} /></AreaChart></ResponsiveContainer></div></div>
                <div className="bg-white p-6 rounded-xl border border-slate-100"><h3 className="text-lg font-semibold mb-2">Self vs Colleague Share ({selfColleague.granularity})</h3><p className="text-sm text-slate-500 mb-6">100% stacked bars by interaction share.</p><div className="h-96"><ResponsiveContainer width="100%" height="100%"><BarChart data={selfColleague.series}><CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" /><XAxis dataKey="bucket_start" tickFormatter={formatBucketLabel} /><YAxis domain={[0, 100]} tickFormatter={(v) => `${v}%`} /><RechartsTooltip /><Legend /><Bar dataKey="self_share_pct" stackId="share" fill="#2563eb" name="Self" /><Bar dataKey="colleague_share_pct" stackId="share" fill="#f97316" name="Colleague" /></BarChart></ResponsiveContainer></div></div>
              </section>
            )}

            {view === 'cost' && pareto && efficiency && (
              <section className="space-y-6">
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                  <div className="bg-white p-6 rounded-xl border border-slate-100 md:col-span-1"><p className="text-sm text-slate-500">Avg Cost per Colleague Solution</p><h3 className="text-3xl font-bold mt-2">{formatCurrency(efficiency.avg_cost_per_colleague_solution, efficiency.currency)}</h3><p className="text-xs text-slate-500 mt-2">Helpful colleague solutions: {efficiency.colleague_helpful_solutions}</p></div>
                  <div className="bg-white p-6 rounded-xl border border-slate-100 md:col-span-2"><p className="text-sm text-slate-500">Top-20% users contribution</p><h3 className="text-3xl font-bold mt-2">{top20Share.toFixed(1)}%</h3><p className="text-xs text-slate-500 mt-2">Pricing: {pareto.pricing.input_price_per_1k_tokens}/1k input, {pareto.pricing.output_price_per_1k_tokens}/1k output ({pareto.currency})</p><p className="text-xs text-slate-500 mt-1">Token proxy mode: {pareto.pricing.is_token_proxy ? 'enabled' : 'disabled'}</p></div>
                </div>
                <div className="bg-white p-6 rounded-xl border border-slate-100"><h3 className="text-lg font-semibold mb-2">Pareto Cost Distribution</h3><p className="text-sm text-slate-500 mb-6">Bars show user cost share; line shows cumulative contribution.</p><div className="h-96"><ResponsiveContainer width="100%" height="100%"><ComposedChart data={pareto.series}><CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" /><XAxis dataKey="rank" /><YAxis yAxisId="left" domain={[0, 100]} tickFormatter={(v) => `${v}%`} /><YAxis yAxisId="right" orientation="right" domain={[0, 100]} tickFormatter={(v) => `${v}%`} /><RechartsTooltip /><Legend /><Bar yAxisId="left" dataKey="cost_share_pct" name="Cost Share %" fill="#3b82f6" /><Line yAxisId="right" dataKey="cumulative_cost_share_pct" name="Cumulative %" stroke="#dc2626" dot={false} /></ComposedChart></ResponsiveContainer></div></div>
              </section>
            )}

            {view === 'quality' && qualityOverview && qualityRiskSelected && (
              <section className="space-y-6">
                <div className="bg-white p-6 rounded-xl border border-slate-100">
                  <h3 className="text-lg font-semibold mb-2">Overall Health Trend ({qualityOverview.health_trend_granularity})</h3>
                  <p className="text-sm text-slate-500 mb-6">Thumb-down rate = thumb-down interactions / total interactions.</p>
                  <div className="h-96">
                    <ResponsiveContainer width="100%" height="100%">
                      <LineChart data={qualityOverview.health_trend}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                        <XAxis dataKey="bucket_start" tickFormatter={formatBucketLabel} />
                        <YAxis tickFormatter={(v) => `${v}%`} />
                        <RechartsTooltip />
                        <Legend />
                        <Line dataKey="down_rate_pct" name="Thumb-down Rate %" stroke="#dc2626" strokeWidth={2} dot={false} />
                      </LineChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                <div className="bg-white p-6 rounded-xl border border-slate-100">
                  <h3 className="text-lg font-semibold mb-2">Defect Breakdown ({qualityOverview.defect_breakdown_granularity})</h3>
                  <p className="text-sm text-slate-500 mb-6">Absolute defect counts by category (non-100% stacked).</p>
                  <div className="h-96">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={qualityOverview.defect_breakdown}>
                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#e2e8f0" />
                        <XAxis dataKey="bucket_start" tickFormatter={formatBucketLabel} />
                        <YAxis />
                        <RechartsTooltip />
                        <Legend />
                        <Bar dataKey="Hallucination" stackId="defects" fill="#ef4444" />
                        <Bar dataKey="OutdatedInfo" stackId="defects" fill="#f59e0b" />
                        <Bar dataKey="Tone" stackId="defects" fill="#3b82f6" />
                        <Bar dataKey="InstructionsUnfollowed" stackId="defects" fill="#8b5cf6" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </div>

                <div className="bg-white p-6 rounded-xl border border-slate-100">
                  <h3 className="text-lg font-semibold mb-2">Department Risk</h3>
                  <p className="text-sm text-slate-500 mb-6">Ranked by thumb-down rate (owner department). Click a bar to drill down.</p>
                  <div className="grid grid-cols-1 xl:grid-cols-2 gap-6">
                    <div className="h-96">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart layout="vertical" data={qualityRiskSelected.ranking}>
                          <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
                          <XAxis type="number" tickFormatter={(v) => `${v}%`} />
                          <YAxis type="category" dataKey="department" width={110} />
                          <RechartsTooltip />
                          <Bar dataKey="thumb_down_rate_pct" name="Thumb-down Rate %" onClick={handleDepartmentBarClick}>
                            {qualityRiskSelected.ranking.map((entry: any) => (
                              <Cell key={entry.department} fill={qualityRiskSelected.selected_department === entry.department ? '#dc2626' : '#3b82f6'} />
                            ))}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    </div>

                    <div className="h-96">
                      <h4 className="text-sm font-semibold text-slate-700 mb-3">
                        {qualityRiskSelected.selected_department ? `${qualityRiskSelected.selected_department} Defect Tags` : 'Department Defect Tags'}
                      </h4>
                      <ResponsiveContainer width="100%" height="90%">
                        <BarChart layout="vertical" data={qualityRiskSelected.selected_breakdown} margin={{ left: 8, right: 8 }}>
                          <CartesianGrid strokeDasharray="3 3" horizontal={false} stroke="#e2e8f0" />
                          <XAxis type="number" allowDecimals={false} />
                          <YAxis type="category" dataKey="category" width={170} tickFormatter={formatDiagnosticCategory} />
                          <RechartsTooltip labelFormatter={(label) => formatDiagnosticCategory(String(label))} />
                          <Bar dataKey="count" fill="#f97316" name="Tag Count" />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </div>
                </div>
              </section>
            )}
          </section>
        </div>
      </main>
    </div>
  );
}
