"use client";

import { use, useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { migrationsApi, type RunSummary, type TableState, type RejectionEntry } from "@/lib/api";
import { StatusBadge } from "@/components/features/migrations/status-badge";
import { formatDistanceToNow } from "date-fns";
import { AlertTriangle, CheckCircle2, Clock, Loader2 } from "lucide-react";

export default function RunDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id: runId } = use(params);
  const qc = useQueryClient();

  const { data: summary } = useQuery<RunSummary>({
    queryKey: ["migration", runId],
    queryFn: () => migrationsApi.get(runId),
    refetchInterval: (q) =>
      q.state.data?.status === "running" ? 3000 : false,
  });

  const { data: tables = [] } = useQuery<TableState[]>({
    queryKey: ["migration-tables", runId],
    queryFn: () => migrationsApi.getTableStates(runId),
    refetchInterval: summary?.status === "running" ? 3000 : false,
  });

  const { data: rejections = [] } = useQuery<RejectionEntry[]>({
    queryKey: ["migration-rejections", runId],
    queryFn: () => migrationsApi.getRejections(runId),
    enabled: (summary?.rows_rejected ?? 0) > 0,
  });

  // SSE subscription for live updates
  useEffect(() => {
    if (!summary || summary.status !== "running") return;
    const es = new EventSource(`/api/migrations/${runId}/stream`);
    es.onmessage = () => {
      qc.invalidateQueries({ queryKey: ["migration", runId] });
      qc.invalidateQueries({ queryKey: ["migration-tables", runId] });
    };
    es.onerror = () => es.close();
    return () => es.close();
  }, [runId, summary?.status, qc]);

  if (!summary) {
    return (
      <div className="flex items-center justify-center h-full">
        <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
      </div>
    );
  }

  const pct =
    summary.tables_total > 0
      ? Math.round((summary.tables_completed / summary.tables_total) * 100)
      : 0;

  return (
    <div className="p-8 space-y-6 max-w-5xl">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div>
          <h1 className="text-2xl font-bold">
            Run #{summary.run_number}
          </h1>
          <p className="text-muted-foreground text-sm mt-1">
            {summary.run_type} · {summary.run_id.slice(0, 8)}…
          </p>
        </div>
        <StatusBadge status={summary.status} />
      </div>

      {/* Stats cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        <StatCard label="Rows Loaded" value={summary.rows_loaded.toLocaleString()} icon={<CheckCircle2 className="w-4 h-4 text-green-500" />} />
        <StatCard label="Rows Rejected" value={summary.rows_rejected.toLocaleString()} icon={<AlertTriangle className="w-4 h-4 text-destructive" />} />
        <StatCard label="Tables Done" value={`${summary.tables_completed}/${summary.tables_total}`} icon={<CheckCircle2 className="w-4 h-4 text-primary" />} />
        <StatCard label="Elapsed" value={summary.elapsed_seconds != null ? `${Math.round(summary.elapsed_seconds)}s` : "—"} icon={<Clock className="w-4 h-4 text-muted-foreground" />} />
      </div>

      {/* Progress bar */}
      {summary.status === "running" && (
        <div className="space-y-1">
          <div className="flex justify-between text-sm text-muted-foreground">
            <span>Progress</span>
            <span>{pct}%</span>
          </div>
          <div className="h-2 bg-muted rounded-full overflow-hidden">
            <div
              className="h-full bg-primary transition-all duration-500 rounded-full"
              style={{ width: `${pct}%` }}
            />
          </div>
        </div>
      )}

      {/* Table states */}
      <div>
        <h2 className="font-semibold mb-3">Tables</h2>
        <div className="bg-card border rounded-xl overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-muted/50 text-muted-foreground">
              <tr>
                <th className="px-4 py-3 text-left">Table</th>
                <th className="px-4 py-3 text-left">Status</th>
                <th className="px-4 py-3 text-right">Loaded</th>
                <th className="px-4 py-3 text-right">Rejected</th>
                <th className="px-4 py-3 text-left">Error</th>
              </tr>
            </thead>
            <tbody>
              {tables.map((t) => (
                <tr key={t.id} className="border-t">
                  <td className="px-4 py-3 font-mono">{t.table_name}</td>
                  <td className="px-4 py-3">
                    <StatusBadge status={t.status} />
                  </td>
                  <td className="px-4 py-3 text-right font-mono">{t.rows_loaded.toLocaleString()}</td>
                  <td className="px-4 py-3 text-right font-mono text-destructive">{t.rows_rejected.toLocaleString()}</td>
                  <td className="px-4 py-3 text-destructive text-xs truncate max-w-[200px]">
                    {t.error_message ?? ""}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Rejection log */}
      {rejections.length > 0 && (
        <div>
          <h2 className="font-semibold mb-3 text-destructive">
            Rejection Log ({rejections.length})
          </h2>
          <div className="bg-card border rounded-xl overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 text-muted-foreground">
                <tr>
                  <th className="px-4 py-3 text-left">Line</th>
                  <th className="px-4 py-3 text-left">Error Type</th>
                  <th className="px-4 py-3 text-left">Message</th>
                </tr>
              </thead>
              <tbody>
                {rejections.map((r) => (
                  <tr key={r.id} className="border-t">
                    <td className="px-4 py-3 font-mono">{r.source_line_num ?? "—"}</td>
                    <td className="px-4 py-3">{r.error_type ?? "—"}</td>
                    <td className="px-4 py-3 text-xs text-destructive truncate max-w-[300px]">
                      {r.error_message}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function StatCard({
  label,
  value,
  icon,
}: {
  label: string;
  value: string;
  icon: React.ReactNode;
}) {
  return (
    <div className="bg-card border rounded-xl p-4 space-y-2">
      <div className="flex items-center gap-2 text-xs text-muted-foreground">
        {icon} {label}
      </div>
      <p className="text-2xl font-bold">{value}</p>
    </div>
  );
}
