"use client";

import { useQuery } from "@tanstack/react-query";
import Link from "next/link";
import { Plus, Database, FileCode, Calendar } from "lucide-react";
import { projectsApi, type Project } from "@/lib/api";
import { formatDistanceToNow } from "date-fns";

export default function ProjectsPage() {
  const { data: projects = [], isLoading } = useQuery({
    queryKey: ["projects"],
    queryFn: projectsApi.list,
  });

  return (
    <div className="p-8 space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Projects</h1>
          <p className="text-muted-foreground text-sm mt-1">
            Manage your COBOL migration projects
          </p>
        </div>
        <Link
          href="/projects/new"
          className="inline-flex items-center gap-2 bg-primary text-primary-foreground px-4 py-2 rounded-lg text-sm font-medium hover:bg-primary/90"
        >
          <Plus className="w-4 h-4" /> New Project
        </Link>
      </div>

      {isLoading ? (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {[1, 2, 3].map((i) => (
            <div key={i} className="h-40 bg-muted rounded-xl animate-pulse" />
          ))}
        </div>
      ) : projects.length === 0 ? (
        <EmptyState />
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {projects.map((p) => (
            <ProjectCard key={p.id} project={p} />
          ))}
        </div>
      )}
    </div>
  );
}

function ProjectCard({ project }: { project: Project }) {
  return (
    <Link
      href={`/projects/${project.id}`}
      className="block p-6 bg-card border rounded-xl hover:shadow-md transition-shadow space-y-4"
    >
      <div className="flex items-start justify-between">
        <div>
          <h3 className="font-semibold">{project.name}</h3>
          {project.description && (
            <p className="text-sm text-muted-foreground mt-1 line-clamp-2">
              {project.description}
            </p>
          )}
        </div>
        <span className="text-xs bg-primary/10 text-primary px-2 py-1 rounded">
          {project.target_type}
        </span>
      </div>

      <div className="flex items-center gap-4 text-xs text-muted-foreground">
        <span className="flex items-center gap-1">
          <FileCode className="w-3 h-3" /> {project.source_type}
        </span>
        <span className="flex items-center gap-1">
          <Database className="w-3 h-3" /> {project.target_type}
        </span>
      </div>

      <div className="flex items-center gap-1 text-xs text-muted-foreground">
        <Calendar className="w-3 h-3" />
        {formatDistanceToNow(new Date(project.created_at), { addSuffix: true })}
      </div>
    </Link>
  );
}

function EmptyState() {
  return (
    <div className="text-center py-20 space-y-4">
      <Database className="w-12 h-12 mx-auto text-muted-foreground" />
      <div>
        <p className="font-semibold">No projects yet</p>
        <p className="text-sm text-muted-foreground">
          Create a project to start migrating your COBOL systems.
        </p>
      </div>
      <Link
        href="/projects/new"
        className="inline-flex items-center gap-2 bg-primary text-primary-foreground px-4 py-2 rounded-lg text-sm font-medium"
      >
        <Plus className="w-4 h-4" /> Create First Project
      </Link>
    </div>
  );
}
