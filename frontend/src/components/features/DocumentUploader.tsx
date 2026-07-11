"use client";

import { useState, useRef } from "react";
import { uploadDocument } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { UploadCloud, FileText, Loader2 } from "lucide-react";

interface DocumentUploaderProps {
  onUploadSuccess: (documentId: string) => void;
}

export function DocumentUploader({ onUploadSuccess }: DocumentUploaderProps) {
  const [isDragging, setIsDragging] = useState(false);
  const [isUploading, setIsUploading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const handleDragLeave = () => {
    setIsDragging(false);
  };

  const handleDrop = async (e: React.DragEvent) => {
    e.preventDefault();
    setIsDragging(false);
    
    if (e.dataTransfer.files && e.dataTransfer.files.length > 0) {
      await processFile(e.dataTransfer.files[0]);
    }
  };

  const handleFileChange = async (e: React.ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      await processFile(e.target.files[0]);
    }
  };

  const processFile = async (file: File) => {
    if (file.type !== "application/pdf" && file.type !== "text/plain") {
      setError("Please upload a PDF or TXT file.");
      return;
    }
    
    setError(null);
    setIsUploading(true);
    
    try {
      const response = await uploadDocument(file);
      onUploadSuccess(response.document_id);
    } catch (err) {
      setError("Failed to upload document. Please ensure the backend is running.");
      console.error(err);
    } finally {
      setIsUploading(false);
    }
  };

  return (
    <Card className="w-full max-w-2xl mx-auto overflow-hidden border-dashed border-2 bg-transparent transition-colors hover:bg-slate-50/5 dark:hover:bg-slate-900/50">
      <CardContent className="p-0">
        <div
          className={`flex flex-col items-center justify-center p-12 text-center transition-all ${
            isDragging ? "border-primary bg-primary/5" : "border-muted"
          }`}
          onDragOver={handleDragOver}
          onDragLeave={handleDragLeave}
          onDrop={handleDrop}
        >
          {isUploading ? (
            <Loader2 className="h-12 w-12 text-primary animate-spin mb-4" />
          ) : (
            <UploadCloud className="h-12 w-12 text-muted-foreground mb-4" />
          )}
          
          <h3 className="text-lg font-semibold mb-2">
            {isUploading ? "Uploading document..." : "Upload your research draft"}
          </h3>
          
          <p className="text-sm text-muted-foreground mb-6 max-w-sm">
            Drag and drop your PDF or TXT file here to extract and verify academic claims against the database.
          </p>
          
          <input
            type="file"
            ref={fileInputRef}
            className="hidden"
            accept=".pdf,.txt"
            onChange={handleFileChange}
          />
          
          <Button 
            onClick={() => fileInputRef.current?.click()}
            disabled={isUploading}
            variant="secondary"
          >
            <FileText className="mr-2 h-4 w-4" />
            Select File
          </Button>

          {error && (
            <p className="text-sm text-destructive mt-4 font-medium">{error}</p>
          )}
        </div>
      </CardContent>
    </Card>
  );
}
