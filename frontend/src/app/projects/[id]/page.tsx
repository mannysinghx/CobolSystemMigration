"use client";

import { use, useRef, useState } from "react";
import { useQuery, useMutation, useQueryClient } from "@tanstack/react-query";
import { useRouter } from "next/navigation";
import { projectsApi, schemaApi, migrationsApi } from "@/lib/api";
import { Upload, Play, Code, FileText } from "lucide-react";
import { StatusBadge } from "@/components/features/migrations/status-badge";

export default function ProjectDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id: projectId } = use(params);
  const router = useRouter();
  const qc = useQueryClient();
  const cbInputRef = useRef<HTMLInputElement>(null);

  const { data: project } = useQuery({
    queryKey: ["project", projectId],
    queryFn: () => projectsApi.get(projectId),
  });

  const { data: copybooks = [] } = useQuery({
    queryKey: ["copybooks", projectId],
    queryFn: () => projectsApi.listCopybooks(projectId),
  });

  const { data: runs = [] } = useQuery({
    queryKey: ["migrations", projectId],
    queryFn: () => migrationsApi.list(projectId),
  });

  const uploadCopybook = useMutation({
    mutationFn: (file: File) => projectsApi.uploadCopybook(projectId, file),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["copybooks", projectId] }),
  });

  const parseCopybook = useMutation({
    mutationFn: (cbId: string) => schemaApi.parse(cbId),
    onSuccess: () => qc.invalidateQueries({ queryKey: ["copybooks", projectId] }),
  });

  const startMigration = useMutation({
    mutationFn: () => migrationsApi.start(projectId),
    onSuccess: (run) => router.push(`/migrations/${run.id}`),
  });

  if (!project) return null;

  return (
    <div className="p-8 space-y-8 max-w-4xl">
      {/* Header */}
      <div className="space-y-1">
        <h1 className="text-2xl font-bold">{project.name}</h1>
        {project.description && (
          <p className="text-muted-foreground">{project.description}</p>
        )}
        <div className="flex gap-2 text-xs text-muted-foreground">
          <span>{project.source_type}</span>
          <span>→</span>
          <span className="font-medium text-primary">{project.target_type}</span>
        </div>
      </div>

      {/* Copybooks section */}
      <section className="space-y-3">
        <div className="flex items-center justify-between">
          <h2 className="font-semibold">Copybooks</h2>
          <button
            onClick={() => cbInputRef.current?.click()}
            className="inline-flex items-center gap-2 text-sm bg-muted px-3 py-1.5 rounded-lg hover:bg-muted/80"
          >
            <Upload className="w-3 h-3" /> Upload .cpy
          </button>
          <input
            ref={cbInputRef}
            type="file"
            accept=".cpy,.cob,.cbl"
            className="hidden"
            onChange={(e) => {
              const file = e.target.files?.[0];
              if (file) uploadCopybook.mutate(file);
            }}
          />
        </div>

        {copybooks.length === 0 ? (
          <p className="text-sm text-muted-foreground">No copybooks uploaded yet.</p>
        ) : (
          <div className="space-y-2">
            {copybooks.map((cb) => (
              <div
                key={cb.id}
                className="flex items-center justify-between bg-card border rounded-lg px-4 py-3"
              >
                <div className="flex items-center gap-3">
                  <FileText className="w-4 h-4 text-muted-foreground" />
                  <div>
                    <p className="text-sm font-medium">{cb.filename}</p>
                    <p className="text-xs text-muted-foreground">
                      {cb.parsed_at ? "Parsed" : "Not parsed"}
                      {cb.parse_errors && cb.parse_errors.length > 0 && (
                        <span className="text-destructive ml-2">
                          {cb.parse_errors.length} error(s)
                        </span>
                      )}
                    </p>
                  </div>
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => parseCopybook.mutate(cb.id)}
                    disabled={parseCopybook.isPending}
                    className="text-xs bg-primary/10 text-primary px-3 py-1 rounded hover:bg-primary/20"
                  >
                    Parse
                  </button>
                  {cb.schema_ir && (
                    <button
                      onClick={() =>
                        router.push(`/schema?copybook_id=${cb.id}&project_id=${projectId}`)
                      }
                      className="text-xs bg-muted px-3 py-1 rounded hover:bg-muted/80 flex items-center gap-1"
                    >
                      <Code className="w-3 h-3" /> Schema
                    </button>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </section>

      {/* Start migration */}
      <section className="space-y-3">
        <h2 className="font-semibold">Run Migration</h2>
        <button
          onClick={() => startMigration.mutate()}
          disabled={startMigration.isPending || copybooks.length === 0}
          className="inline-flex items-center gap-2 bg-primary text-primary-foreground px-5 py-2.5 rounded-lg font-medium hover:bg-primary/90 disabled:opacity-50"
        >
          <Play className="w-4 h-4" />
          {startMigration.isPending ? "Starting…" : "Start Migration"}
        </button>
        {copybooks.length === 0 && (
          <p className="text-xs text-muted-foreground">Upload at least one copybook first.</p>
        )}
      </section>

      {/* Recent runs */}
      {runs.length > 0 && (
        <section className="space-y-3">
          <h2 className="font-semibold">Recent Runs</h2>
          <div className="bg-card border rounded-xl overflow-hidden">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 text-muted-foreground">
                <tr>
                  <th className="px-4 py-3 text-left">Run #</th>
                  <th className="px-4 py-3 text-left">Status</th>
                  <th className="px-4 py-3 text-right">Loaded</th>
                  <th className="px-4 py-3 text-right">Rejected</th>
                </tr>
              </thead>
              <tbody>
                {runs.slice(0, 5).map((run) => (
                  <tr
                    key={run.id}
                    className="border-t hover:bg-muted/20 cursor-pointer"
                    onClick={() => router.push(`/migrations/${run.id}`)}
                  >
                    <td className="px-4 py-3 font-mono">#{run.run_number}</td>
                    <td className="px-4 py-3">
                      <StatusBadge status={run.status} />
                    </td>
                    <td className="px-4 py-3 text-right font-mono">{run.rows_loaded.toLocaleString()}</td>
                    <td className="px-4 py-3 text-right font-mono text-destructive">{run.rows_rejected.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}
    </div>
  );
}
