"use client";

import { signIn, signOut, useSession } from "next-auth/react";
import { Button } from "@/components/ui/button";
import Link from "next/link";
import { User, LogOut } from "lucide-react";

export default function AuthButton() {
  const { data: session } = useSession();

  if (session) {
    return (
      <div className="flex items-center gap-4">
        <Link href="/dashboard">
          <Button variant="ghost" size="sm" className="gap-2">
            <User className="w-4 h-4" />
            Dashboard
          </Button>
        </Link>
        <Button variant="outline" size="sm" onClick={() => signOut()} className="gap-2">
          <LogOut className="w-4 h-4" />
          Sign Out
        </Button>
      </div>
    );
  }

  return (
    <Button size="sm" onClick={() => signIn()} className="gap-2">
      <User className="w-4 h-4" />
      Sign In
    </Button>
  );
}
