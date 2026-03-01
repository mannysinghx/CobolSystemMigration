"use client";

import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import { migrationsApi, projectsApi } from "@/lib/api";
import { StatusBadge } from "@/components/features/migrations/status-badge";
import { formatDistanceToNow } from "date-fns";

export default function MigrationsPage() {
  const { data: projects = [] } = useQuery({
    queryKey: ["projects"],
    queryFn: projectsApi.list,
  });

  return (
    <div className="p-8 space-y-6">
      <div>
        <h1 className="text-2xl font-bold">Migrations</h1>
        <p className="text-muted-foreground text-sm mt-1">
          All migration runs across all projects
        </p>
      </div>

      <div className="space-y-4">
        {projects.map((project) => (
          <ProjectRunsSection key={project.id} projectId={project.id} projectName={project.name} />
        ))}
      </div>
    </div>
  );
}

function ProjectRunsSection({
  projectId,
  projectName,
}: {
  projectId: string;
  projectName: string;
}) {
  const { data: runs = [] } = useQuery({
    queryKey: ["migrations", projectId],
    queryFn: () => migrationsApi.list(projectId),
  });

  if (runs.length === 0) return null;

  return (
    <div className="space-y-2">
      <h2 className="text-sm font-semibold text-muted-foreground uppercase tracking-wide">
        {projectName}
      </h2>
      <div className="bg-card border rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead className="bg-muted/50 text-muted-foreground">
            <tr>
              <th className="px-4 py-3 text-left">Run #</th>
              <th className="px-4 py-3 text-left">Type</th>
              <th className="px-4 py-3 text-left">Status</th>
              <th className="px-4 py-3 text-right">Loaded</th>
              <th className="px-4 py-3 text-right">Rejected</th>
              <th className="px-4 py-3 text-left">Started</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => (
              <tr
                key={run.id}
                className="border-t hover:bg-muted/20 cursor-pointer"
                onClick={() => (window.location.href = `/migrations/${run.id}`)}
              >
                <td className="px-4 py-3 font-mono">#{run.run_number}</td>
                <td className="px-4 py-3">{run.run_type}</td>
                <td className="px-4 py-3">
                  <StatusBadge status={run.status} />
                </td>
                <td className="px-4 py-3 text-right font-mono">{run.rows_loaded.toLocaleString()}</td>
                <td className="px-4 py-3 text-right font-mono text-destructive">
                  {run.rows_rejected.toLocaleString()}
                </td>
                <td className="px-4 py-3 text-muted-foreground">
                  {run.started_at
                    ? formatDistanceToNow(new Date(run.started_at), { addSuffix: true })
                    : "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
