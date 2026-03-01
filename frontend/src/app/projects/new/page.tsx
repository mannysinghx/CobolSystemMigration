"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { projectsApi } from "@/lib/api";

export default function NewProjectPage() {
  const router = useRouter();
  const qc = useQueryClient();

  const [form, setForm] = useState({
    name: "",
    description: "",
    source_type: "vsam_flat",
    target_type: "postgresql",
  });
  const [error, setError] = useState("");

  const create = useMutation({
    mutationFn: () => projectsApi.create(form),
    onSuccess: (project) => {
      qc.invalidateQueries({ queryKey: ["projects"] });
      router.push(`/projects/${project.id}`);
    },
    onError: (e: Error) => setError(e.message),
  });

  function handleSubmit(e: React.FormEvent) {
    e.preventDefault();
    if (!form.name.trim()) { setError("Project name is required"); return; }
    create.mutate();
  }

  return (
    <div className="p-8 max-w-lg">
      <h1 className="text-2xl font-bold mb-6">New Project</h1>

      <form onSubmit={handleSubmit} className="space-y-4">
        <Field label="Project Name" required>
          <input
            className="input"
            value={form.name}
            onChange={(e) => setForm({ ...form, name: e.target.value })}
            placeholder="e.g. Customer Master Migration"
          />
        </Field>

        <Field label="Description">
          <textarea
            className="input min-h-[80px]"
            value={form.description}
            onChange={(e) => setForm({ ...form, description: e.target.value })}
            placeholder="Optional description…"
          />
        </Field>

        <Field label="Source Type">
          <select
            className="input"
            value={form.source_type}
            onChange={(e) => setForm({ ...form, source_type: e.target.value })}
          >
            <option value="vsam_flat">VSAM Flat File</option>
            <option value="db2">DB2</option>
            <option value="ims">IMS</option>
            <option value="mixed">Mixed</option>
          </select>
        </Field>

        <Field label="Target Database">
          <select
            className="input"
            value={form.target_type}
            onChange={(e) => setForm({ ...form, target_type: e.target.value })}
          >
            <option value="postgresql">PostgreSQL</option>
            <option value="sqlserver">SQL Server</option>
          </select>
        </Field>

        {error && <p className="text-destructive text-sm">{error}</p>}

        <button
          type="submit"
          disabled={create.isPending}
          className="w-full bg-primary text-primary-foreground py-2 rounded-lg font-medium hover:bg-primary/90 disabled:opacity-50"
        >
          {create.isPending ? "Creating…" : "Create Project"}
        </button>
      </form>
    </div>
  );
}

function Field({
  label,
  required,
  children,
}: {
  label: string;
  required?: boolean;
  children: React.ReactNode;
}) {
  return (
    <div className="space-y-1">
      <label className="text-sm font-medium">
        {label} {required && <span className="text-destructive">*</span>}
      </label>
      {children}
    </div>
  );
}
