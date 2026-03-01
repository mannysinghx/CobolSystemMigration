"use client";

import { useSearchParams } from "next/navigation";
import { useQuery, useMutation } from "@tanstack/react-query";
import { projectsApi, schemaApi } from "@/lib/api";
import { Suspense, useState } from "react";
import { Copy, Download } from "lucide-react";

export default function SchemaPage() {
  return (
    <Suspense>
      <SchemaContent />
    </Suspense>
  );
}

function SchemaContent() {
  const params = useSearchParams();
  const copybookId = params.get("copybook_id");
  const [dialect, setDialect] = useState<"postgresql" | "sqlserver">("postgresql");
  const [ddlSql, setDdlSql] = useState<string>("");
  const [copied, setCopied] = useState(false);

  const { data: copybook } = useQuery({
    queryKey: ["schema-ir", copybookId],
    queryFn: async () => {
      if (!copybookId) return null;
      // Fetch the copybook details via projects API
      // For now we use schema parse endpoint
      return schemaApi.parse(copybookId);
    },
    enabled: !!copybookId,
  });

  const generateDDL = useMutation({
    mutationFn: () => schemaApi.generateDDL(copybookId!, dialect),
    onSuccess: (data) => setDdlSql(data.sql),
  });

  function copyToClipboard() {
    navigator.clipboard.writeText(ddlSql);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  }

  function downloadSql() {
    const blob = new Blob([ddlSql], { type: "text/sql" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `schema_${dialect}.sql`;
    a.click();
    URL.revokeObjectURL(url);
  }

  if (!copybookId) {
    return (
      <div className="p-8">
        <h1 className="text-2xl font-bold mb-2">Schema Explorer</h1>
        <p className="text-muted-foreground">
          Select a copybook from a project to view its schema.
        </p>
      </div>
    );
  }

  return (
    <div className="p-8 space-y-6 max-w-5xl">
      <h1 className="text-2xl font-bold">Schema Explorer</h1>

      {/* DDL Generator */}
      <div className="bg-card border rounded-xl p-6 space-y-4">
        <h2 className="font-semibold">Generate DDL</h2>
        <div className="flex gap-3 items-center">
          <select
            value={dialect}
            onChange={(e) => setDialect(e.target.value as "postgresql" | "sqlserver")}
            className="input w-40"
          >
            <option value="postgresql">PostgreSQL</option>
            <option value="sqlserver">SQL Server</option>
          </select>
          <button
            onClick={() => generateDDL.mutate()}
            disabled={generateDDL.isPending}
            className="bg-primary text-primary-foreground px-4 py-2 rounded-lg text-sm font-medium hover:bg-primary/90 disabled:opacity-50"
          >
            {generateDDL.isPending ? "Generating…" : "Generate DDL"}
          </button>
        </div>

        {ddlSql && (
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <span className="text-sm text-muted-foreground">Output SQL</span>
              <div className="flex gap-2">
                <button
                  onClick={copyToClipboard}
                  className="flex items-center gap-1 text-xs bg-muted px-3 py-1.5 rounded hover:bg-muted/80"
                >
                  <Copy className="w-3 h-3" /> {copied ? "Copied!" : "Copy"}
                </button>
                <button
                  onClick={downloadSql}
                  className="flex items-center gap-1 text-xs bg-muted px-3 py-1.5 rounded hover:bg-muted/80"
                >
                  <Download className="w-3 h-3" /> Download
                </button>
              </div>
            </div>
            <pre className="bg-muted/50 rounded-lg p-4 text-xs font-mono overflow-auto max-h-[500px] whitespace-pre">
              {ddlSql}
            </pre>
          </div>
        )}
      </div>

      {/* Schema IR tree */}
      {copybook?.schema && (
        <div className="bg-card border rounded-xl p-6 space-y-4">
          <h2 className="font-semibold">Schema IR</h2>
          <pre className="bg-muted/50 rounded-lg p-4 text-xs font-mono overflow-auto max-h-[400px]">
            {JSON.stringify(copybook.schema, null, 2)}
          </pre>
        </div>
      )}
    </div>
  );
}
