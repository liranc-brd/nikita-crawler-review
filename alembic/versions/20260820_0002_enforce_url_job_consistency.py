"""enforce URL job consistency

Revision ID: 20260820_0002
Revises: 20260820_0001
Create Date: 2026-08-20 00:02:00
"""
from __future__ import annotations

from alembic import op


revision = "20260820_0002"
down_revision = "20260820_0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_unique_constraint("uq_crawl_urls_id_job_id", "crawl_urls", ["id", "job_id"])

    op.drop_constraint("content_artifacts_crawl_url_id_fkey", "content_artifacts", type_="foreignkey")
    op.create_foreign_key(
        "fk_content_artifacts_crawl_url_job_id",
        "content_artifacts",
        "crawl_urls",
        ["crawl_url_id", "job_id"],
        ["id", "job_id"],
        ondelete="CASCADE",
    )

    op.drop_constraint("discovered_links_source_url_id_fkey", "discovered_links", type_="foreignkey")
    op.drop_constraint("discovered_links_target_url_id_fkey", "discovered_links", type_="foreignkey")
    op.create_foreign_key(
        "fk_discovered_links_source_url_job_id",
        "discovered_links",
        "crawl_urls",
        ["source_url_id", "job_id"],
        ["id", "job_id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "fk_discovered_links_target_url_job_id",
        "discovered_links",
        "crawl_urls",
        ["target_url_id", "job_id"],
        ["id", "job_id"],
        ondelete="SET NULL (target_url_id)",
    )


def downgrade() -> None:
    op.drop_constraint("fk_discovered_links_target_url_job_id", "discovered_links", type_="foreignkey")
    op.drop_constraint("fk_discovered_links_source_url_job_id", "discovered_links", type_="foreignkey")
    op.create_foreign_key(
        "discovered_links_target_url_id_fkey",
        "discovered_links",
        "crawl_urls",
        ["target_url_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "discovered_links_source_url_id_fkey",
        "discovered_links",
        "crawl_urls",
        ["source_url_id"],
        ["id"],
        ondelete="CASCADE",
    )

    op.drop_constraint("fk_content_artifacts_crawl_url_job_id", "content_artifacts", type_="foreignkey")
    op.create_foreign_key(
        "content_artifacts_crawl_url_id_fkey",
        "content_artifacts",
        "crawl_urls",
        ["crawl_url_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_constraint("uq_crawl_urls_id_job_id", "crawl_urls", type_="unique")
