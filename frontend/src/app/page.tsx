"use client";

import { useState } from "react";
import { DocumentUploader } from "@/components/features/DocumentUploader";
import { ClaimList } from "@/components/features/ClaimList";
import { ClaimVerification, verifyDocument } from "@/lib/api";
import { Loader2 } from "lucide-react";

export default function Home() {
  const [documentId, setDocumentId] = useState<string | null>(null);
  const [claims, setClaims] = useState<ClaimVerification[]>([]);
  const [isVerifying, setIsVerifying] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleUploadSuccess = async (id: string) => {
    setDocumentId(id);
    setIsVerifying(true);
    setError(null);
    setClaims([]);

    try {
      const response = await verifyDocument(id);
      setClaims(response.claims);
    } catch (err) {
      console.error(err);
      setError("Failed to verify document claims. Please ensure backend is responding.");
    } finally {
      setIsVerifying(false);
    }
  };

  return (
    <div className="container mx-auto px-4 py-12 md:py-24">
      <div className="flex flex-col items-center justify-center space-y-4 text-center mb-16">
        <h1 className="text-4xl font-extrabold tracking-tight sm:text-5xl md:text-6xl text-transparent bg-clip-text bg-gradient-to-r from-primary to-blue-600 dark:from-white dark:to-slate-400">
          Verify Academic Claims
        </h1>
        <p className="max-w-[700px] text-muted-foreground md:text-xl">
          Upload your manuscript draft. We will automatically extract falsifiable claims and verify them against the global academic database.
        </p>
      </div>

      {!documentId && !isVerifying && (
        <div className="flex justify-center slide-up-fade-in">
          <DocumentUploader onUploadSuccess={handleUploadSuccess} />
        </div>
      )}

      {isVerifying && (
        <div className="flex flex-col items-center justify-center space-y-4 py-20 text-muted-foreground animate-pulse">
          <Loader2 className="w-12 h-12 animate-spin text-primary" />
          <p className="text-lg">Analyzing document and querying OpenSearch index...</p>
        </div>
      )}

      {error && (
        <div className="bg-destructive/10 text-destructive p-4 rounded-lg text-center max-w-2xl mx-auto border border-destructive/20">
          {error}
        </div>
      )}

      {documentId && claims.length > 0 && !isVerifying && (
        <div className="slide-up-fade-in mt-8">
          <ClaimList claims={claims} />
          
          <div className="mt-12 text-center">
            <button 
              onClick={() => {
                setDocumentId(null);
                setClaims([]);
              }}
              className="text-sm text-muted-foreground hover:text-primary transition-colors underline underline-offset-4"
            >
              Upload another document
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
