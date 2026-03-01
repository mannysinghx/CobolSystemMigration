import Link from "next/link";
import { ArrowRight, Database, FileCode, BarChart3 } from "lucide-react";

export default function HomePage() {
  return (
    <div className="flex flex-col items-center justify-center min-h-full p-8">
      <div className="max-w-2xl w-full space-y-8">
        <div className="text-center space-y-3">
          <h1 className="text-4xl font-bold tracking-tight">
            Cobol<span className="text-primary">Shift</span>
          </h1>
          <p className="text-muted-foreground text-lg">
            End-to-end COBOL → PostgreSQL / SQL Server migration platform
          </p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <FeatureCard
            icon={<FileCode className="w-6 h-6" />}
            title="Parse Copybooks"
            description="Upload COBOL copybooks and generate full schema IR with type mapping."
            href="/projects/new"
          />
          <FeatureCard
            icon={<Database className="w-6 h-6" />}
            title="Run Migrations"
            description="Stream flat files through the decoder and bulk-load into your target DB."
            href="/projects"
          />
          <FeatureCard
            icon={<BarChart3 className="w-6 h-6" />}
            title="Track Progress"
            description="Live SSE dashboard with row counts, rejection log, and validation."
            href="/migrations"
          />
        </div>

        <div className="flex justify-center">
          <Link
            href="/projects/new"
            className="inline-flex items-center gap-2 bg-primary text-primary-foreground px-6 py-3 rounded-lg font-medium hover:bg-primary/90 transition-colors"
          >
            New Project <ArrowRight className="w-4 h-4" />
          </Link>
        </div>
      </div>
    </div>
  );
}

function FeatureCard({
  icon,
  title,
  description,
  href,
}: {
  icon: React.ReactNode;
  title: string;
  description: string;
  href: string;
}) {
  return (
    <Link
      href={href}
      className="block p-6 bg-card border rounded-xl hover:shadow-md transition-shadow space-y-3"
    >
      <div className="p-2 bg-primary/10 rounded-lg w-fit text-primary">{icon}</div>
      <h3 className="font-semibold">{title}</h3>
      <p className="text-sm text-muted-foreground">{description}</p>
    </Link>
  );
}
