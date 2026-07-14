export const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://127.0.0.1:8000";

export interface ClaimVerification {
  claim_id: string;
  claim_text: string;
  verification_status: "Supported" | "Refuted" | "Inconclusive";
  evidence: Array<{
    paper_id: string;
    title: string;
    authors: string[];
    year: number;
    abstract_snippet: string;
  }>;
}

export interface VerificationResponse {
  document_id: string;
  claims: ClaimVerification[];
}

export async function uploadDocument(file: File): Promise<{ document_id: string }> {
  const formData = new FormData();
  formData.append("file", file);

  const response = await fetch(`${API_BASE_URL}/v1/documents/upload`, {
    method: "POST",
    body: formData,
  });

  if (!response.ok) {
    throw new Error("Failed to upload document");
  }

  return response.json();
}

export async function verifyDocument(documentId: string): Promise<VerificationResponse> {
  const response = await fetch(`${API_BASE_URL}/v1/documents/${documentId}/verify`, {
    method: "POST",
  });

  if (!response.ok) {
    throw new Error("Failed to verify document");
  }

  return response.json();
}

export async function exportDocument(documentId: string, claims: ClaimVerification[], citationStyle: string = "APA"): Promise<string> {
  const response = await fetch(`${API_BASE_URL}/v1/documents/${documentId}/export`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ claims, citation_style: citationStyle }),
  });

  if (!response.ok) {
    throw new Error("Failed to export document");
  }

  return response.text();
}
