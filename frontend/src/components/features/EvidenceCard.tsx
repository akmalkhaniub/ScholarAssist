import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { BookOpen, Calendar, Users } from "lucide-react";

interface EvidenceCardProps {
  paperId: string;
  title: string;
  authors: string[];
  year: number;
  abstractSnippet: string;
}

export function EvidenceCard({
  title,
  authors,
  year,
  abstractSnippet,
}: EvidenceCardProps) {
  return (
    <Card className="bg-slate-50/50 dark:bg-slate-900/50 shadow-sm">
      <CardHeader className="pb-2">
        <CardTitle className="text-base leading-snug">
          {title}
        </CardTitle>
        <div className="flex flex-wrap gap-3 text-xs text-muted-foreground mt-2">
          <div className="flex items-center gap-1">
            <Users className="w-3 h-3" />
            <span className="truncate max-w-[200px]">{authors.join(", ")}</span>
          </div>
          <div className="flex items-center gap-1">
            <Calendar className="w-3 h-3" />
            <span>{year}</span>
          </div>
          <div className="flex items-center gap-1">
            <BookOpen className="w-3 h-3" />
            <span>OpenSearch Indexed</span>
          </div>
        </div>
      </CardHeader>
      <CardContent>
        <p className="text-sm italic text-muted-foreground bg-muted/30 p-3 rounded-md border border-muted/50">
          "{abstractSnippet}..."
        </p>
      </CardContent>
    </Card>
  );
}
