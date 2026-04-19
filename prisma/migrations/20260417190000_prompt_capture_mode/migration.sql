ALTER TABLE "Organization" ADD COLUMN IF NOT EXISTS "promptCaptureMode" TEXT NOT NULL DEFAULT 'full';
