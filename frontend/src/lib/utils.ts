import { clsx, type ClassValue } from "clsx";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}

const DOMAIN_LABELS: Record<string, string> = {
  ins_policy: "Insurance Policy",
  ins_claims: "Insurance Claims",
  ins_customer: "Insurance Customer",
};

export function domainLabel(domain: string): string {
  return DOMAIN_LABELS[domain] || domain.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
}
