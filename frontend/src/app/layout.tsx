import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { BookOpen } from "lucide-react";
import { Providers } from "@/components/providers";
import AuthButton from "@/components/features/AuthButton";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "ScholarAssist",
  description: "Academic verification platform",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.className} min-h-screen bg-background text-foreground antialiased`}>
        <Providers>
          <div className="relative flex min-h-screen flex-col">
            <header className="sticky top-0 z-50 w-full border-b bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60">
              <div className="container flex h-14 items-center justify-between mx-auto px-4">
                <div className="flex gap-2 items-center font-bold text-lg tracking-tight">
                  <BookOpen className="w-5 h-5 text-primary" />
                  <span>ScholarAssist</span>
                </div>
                <AuthButton />
              </div>
            </header>
            <main className="flex-1">
              {children}
            </main>
          </div>
        </Providers>
      </body>
    </html>
  );
}
