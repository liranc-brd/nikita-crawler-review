"""create crawler schema

Revision ID: 20260820_0001
Revises:
Create Date: 2026-08-20 00:01:00
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


revision = "20260820_0001"
down_revision = None
branch_labels = None
depends_on = None

job_status = postgresql.ENUM(
    "pending", "running", "pausing", "paused", "canceling", "canceled", "completed", "failed",
    name="job_status",
    create_type=False,
)
url_status = postgresql.ENUM(
    "discovered", "queued", "claimed", "fetching", "processing", "retry_wait", "done",
    "failed_permanent", "canceled", "skipped_child_spawned",
    name="url_status",
    create_type=False,
)


def upgrade() -> None:
    job_status.create(op.get_bind(), checkfirst=True)
    url_status.create(op.get_bind(), checkfirst=True)

    op.create_table(
        "crawl_jobs",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("seed_url", sa.String(), nullable=False),
        sa.Column("seed_hostname", sa.String(), nullable=False),
        sa.Column("parent_job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("status", job_status, nullable=False, server_default="pending"),
        sa.Column("config", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("pause_requested_at", sa.DateTime(), nullable=True),
        sa.Column("cancel_requested_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["parent_job_id"], ["crawl_jobs.id"], ondelete="SET NULL"),
    )
    op.create_table(
        "crawl_urls",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("normalized_url", sa.String(), nullable=False),
        sa.Column("url_hash", sa.String(), nullable=False),
        sa.Column("discovered_from_url_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("status", url_status, nullable=False, server_default="discovered"),
        sa.Column("content_type", sa.String(), nullable=True),
        sa.Column("http_status_code", sa.Integer(), nullable=True),
        sa.Column("fetch_attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_eligible_at", sa.DateTime(), nullable=True),
        sa.Column("claimed_by", sa.String(), nullable=True),
        sa.Column("claimed_at", sa.DateTime(), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(), nullable=True),
        sa.Column("last_heartbeat_at", sa.DateTime(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("content_artifact_id", postgresql.UUID(as_uuid=True), nullable=True, unique=True),
        sa.Column("error_code", sa.String(), nullable=True),
        sa.Column("error_detail", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["job_id"], ["crawl_jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["discovered_from_url_id"], ["crawl_urls.id"], ondelete="SET NULL"),
        sa.UniqueConstraint("job_id", "normalized_url", name="uq_crawl_urls_job_id_normalized_url"),
    )
    op.create_table(
        "content_artifacts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("crawl_url_id", postgresql.UUID(as_uuid=True), nullable=False, unique=True),
        sa.Column("content_type", sa.String(), nullable=False),
        sa.Column("storage_path", sa.String(), nullable=False),
        sa.Column("filename", sa.String(), nullable=False),
        sa.Column("content_length", sa.Integer(), nullable=True),
        sa.Column("content_hash", sa.String(), nullable=False),
        sa.Column("etag", sa.String(), nullable=True),
        sa.Column("last_modified", sa.String(), nullable=True),
        sa.Column("saved_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["job_id"], ["crawl_jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["crawl_url_id"], ["crawl_urls.id"], ondelete="CASCADE"),
    )
    op.create_foreign_key(
        "fk_crawl_urls_content_artifact_id",
        "crawl_urls",
        "content_artifacts",
        ["content_artifact_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_table(
        "crawl_attempts",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("crawl_url_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("result_status", sa.String(), nullable=False),
        sa.Column("http_status_code", sa.Integer(), nullable=True),
        sa.Column("retry_after_seconds", sa.Integer(), nullable=True),
        sa.Column("response_headers", postgresql.JSONB(), nullable=True),
        sa.Column("error_detail", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["crawl_url_id"], ["crawl_urls.id"], ondelete="CASCADE"),
    )
    op.create_table(
        "discovered_links",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("job_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("source_url_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_normalized_url", sa.String(), nullable=False),
        sa.Column("target_url_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("is_same_hostname", sa.Boolean(), nullable=False),
        sa.Column("spawned_child_job_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("discovered_at", sa.DateTime(), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["job_id"], ["crawl_jobs.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["source_url_id"], ["crawl_urls.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_url_id"], ["crawl_urls.id"], ondelete="SET NULL"),
        sa.ForeignKeyConstraint(["spawned_child_job_id"], ["crawl_jobs.id"], ondelete="SET NULL"),
    )
    op.create_table(
        "content_metadata",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("artifact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("metadata_type", sa.String(), nullable=False),
        sa.Column("metadata_json", postgresql.JSONB(), nullable=False),
        sa.ForeignKeyConstraint(["artifact_id"], ["content_artifacts.id"], ondelete="CASCADE"),
    )


def downgrade() -> None:
    op.drop_table("content_metadata")
    op.drop_table("discovered_links")
    op.drop_table("crawl_attempts")
    op.drop_constraint("fk_crawl_urls_content_artifact_id", "crawl_urls", type_="foreignkey")
    op.drop_table("content_artifacts")
    op.drop_table("crawl_urls")
    op.drop_table("crawl_jobs")
    url_status.drop(op.get_bind(), checkfirst=True)
    job_status.drop(op.get_bind(), checkfirst=True)
