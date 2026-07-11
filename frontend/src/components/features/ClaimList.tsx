import { ClaimVerification } from "@/lib/api";
import { EvidenceCard } from "./EvidenceCard";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { CheckCircle2, XCircle, AlertCircle } from "lucide-react";

interface ClaimListProps {
  claims: ClaimVerification[];
}

export function ClaimList({ claims }: ClaimListProps) {
  if (!claims.length) {
    return null;
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "Supported":
        return <CheckCircle2 className="w-5 h-5 text-green-500" />;
      case "Refuted":
        return <XCircle className="w-5 h-5 text-destructive" />;
      default:
        return <AlertCircle className="w-5 h-5 text-yellow-500" />;
    }
  };

  const getStatusBadge = (status: string) => {
    switch (status) {
      case "Supported":
        return <Badge variant="outline" className="bg-green-500/10 text-green-600 border-green-500/20">Supported</Badge>;
      case "Refuted":
        return <Badge variant="outline" className="bg-destructive/10 text-destructive border-destructive/20">Refuted</Badge>;
      default:
        return <Badge variant="outline" className="bg-yellow-500/10 text-yellow-600 border-yellow-500/20">Inconclusive</Badge>;
    }
  };

  return (
    <div className="space-y-6 w-full max-w-4xl mx-auto">
      <h2 className="text-2xl font-semibold tracking-tight">Verified Claims ({claims.length})</h2>
      
      {claims.map((claim) => (
        <Card key={claim.claim_id} className="border-l-4 overflow-hidden" 
          style={{ 
            borderLeftColor: claim.verification_status === "Supported" ? "#22c55e" : 
                             claim.verification_status === "Refuted" ? "#ef4444" : "#eab308" 
          }}>
          <CardHeader className="bg-muted/10 pb-4">
            <div className="flex items-start justify-between gap-4">
              <div className="flex items-start gap-3">
                <div className="mt-1">{getStatusIcon(claim.verification_status)}</div>
                <CardTitle className="text-lg leading-relaxed">{claim.claim_text}</CardTitle>
              </div>
              <div>{getStatusBadge(claim.verification_status)}</div>
            </div>
          </CardHeader>
          
          <CardContent className="pt-6">
            {claim.evidence.length > 0 ? (
              <div className="space-y-4">
                <h4 className="text-sm font-medium text-muted-foreground uppercase tracking-wider">
                  Supporting Evidence
                </h4>
                <div className="grid gap-4">
                  {claim.evidence.map((evidence, idx) => (
                    <EvidenceCard
                      key={`${evidence.paper_id}-${idx}`}
                      paperId={evidence.paper_id}
                      title={evidence.title}
                      authors={evidence.authors}
                      year={evidence.year}
                      abstractSnippet={evidence.abstract_snippet}
                    />
                  ))}
                </div>
              </div>
            ) : (
              <p className="text-sm text-muted-foreground italic">
                No indexed evidence found for this claim.
              </p>
            )}
          </CardContent>
        </Card>
      ))}
    </div>
  );
}
