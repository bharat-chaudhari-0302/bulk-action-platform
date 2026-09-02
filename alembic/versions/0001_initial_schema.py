"""Initial schema: accounts, CRM entities, and the bulk action control plane.

The DDL below is the exact rendering of the SQLAlchemy metadata in `app/models`
against the PostgreSQL dialect, so the migration and the models cannot drift.
Subsequent migrations should be produced with `alembic revision --autogenerate`.

Revision ID: 0001
Revises:
Create Date: 2026-09-02
"""

from collections.abc import Sequence

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


TABLES = [
    """
    CREATE TABLE accounts (
        id UUID NOT NULL,
        name VARCHAR(255) NOT NULL,
        rate_limit_per_minute INTEGER DEFAULT '10000' NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        CONSTRAINT pk_accounts PRIMARY KEY (id)
    )
    """,
    """
    CREATE TABLE contacts (
        id UUID NOT NULL,
        account_id UUID NOT NULL,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(320) NOT NULL,
        age INTEGER,
        status VARCHAR(50) DEFAULT 'active' NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        deleted_at TIMESTAMP WITH TIME ZONE,
        CONSTRAINT pk_contacts PRIMARY KEY (id),
        CONSTRAINT fk_contacts_account_id_accounts
            FOREIGN KEY(account_id) REFERENCES accounts (id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE companies (
        id UUID NOT NULL,
        account_id UUID NOT NULL,
        name VARCHAR(255) NOT NULL,
        domain VARCHAR(255),
        industry VARCHAR(100),
        employee_count INTEGER,
        status VARCHAR(50) DEFAULT 'active' NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        deleted_at TIMESTAMP WITH TIME ZONE,
        CONSTRAINT pk_companies PRIMARY KEY (id),
        CONSTRAINT fk_companies_account_id_accounts
            FOREIGN KEY(account_id) REFERENCES accounts (id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE bulk_actions (
        id UUID NOT NULL,
        account_id UUID NOT NULL,
        entity_type VARCHAR(64) NOT NULL,
        action_type VARCHAR(64) NOT NULL,
        status VARCHAR(32) DEFAULT 'queued' NOT NULL,
        configuration JSONB DEFAULT '{}' NOT NULL,
        batch_size INTEGER DEFAULT '1000' NOT NULL,
        scheduled_at TIMESTAMP WITH TIME ZONE,
        total_entities INTEGER DEFAULT '0' NOT NULL,
        processed_count INTEGER DEFAULT '0' NOT NULL,
        success_count INTEGER DEFAULT '0' NOT NULL,
        failure_count INTEGER DEFAULT '0' NOT NULL,
        skipped_count INTEGER DEFAULT '0' NOT NULL,
        total_batches INTEGER DEFAULT '0' NOT NULL,
        completed_batches INTEGER DEFAULT '0' NOT NULL,
        error TEXT,
        idempotency_key VARCHAR(255),
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        started_at TIMESTAMP WITH TIME ZONE,
        finished_at TIMESTAMP WITH TIME ZONE,
        CONSTRAINT pk_bulk_actions PRIMARY KEY (id),
        CONSTRAINT ck_bulk_actions_status_valid CHECK (status IN (
            'scheduled', 'queued', 'planning', 'processing',
            'completed', 'completed_with_errors', 'failed', 'cancelled')),
        CONSTRAINT uq_bulk_actions_idempotency UNIQUE (account_id, idempotency_key),
        CONSTRAINT fk_bulk_actions_account_id_accounts
            FOREIGN KEY(account_id) REFERENCES accounts (id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE bulk_action_batches (
        id UUID NOT NULL,
        bulk_action_id UUID NOT NULL,
        batch_index INTEGER NOT NULL,
        status VARCHAR(32) DEFAULT 'pending' NOT NULL,
        entity_ids UUID[],
        cursor_start UUID,
        cursor_end UUID,
        entity_count INTEGER DEFAULT '0' NOT NULL,
        success_count INTEGER DEFAULT '0' NOT NULL,
        failure_count INTEGER DEFAULT '0' NOT NULL,
        skipped_count INTEGER DEFAULT '0' NOT NULL,
        attempts INTEGER DEFAULT '0' NOT NULL,
        error TEXT,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        started_at TIMESTAMP WITH TIME ZONE,
        finished_at TIMESTAMP WITH TIME ZONE,
        CONSTRAINT pk_bulk_action_batches PRIMARY KEY (id),
        CONSTRAINT ck_bulk_action_batches_status_valid CHECK (status IN (
            'pending', 'processing', 'completed', 'failed', 'cancelled')),
        CONSTRAINT uq_batch_action_index UNIQUE (bulk_action_id, batch_index),
        CONSTRAINT fk_bulk_action_batches_bulk_action_id_bulk_actions
            FOREIGN KEY(bulk_action_id) REFERENCES bulk_actions (id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE bulk_action_logs (
        id BIGSERIAL NOT NULL,
        bulk_action_id UUID NOT NULL,
        batch_id UUID,
        entity_id UUID,
        status VARCHAR(16) NOT NULL,
        reason_code VARCHAR(64),
        message TEXT,
        details JSONB,
        created_at TIMESTAMP WITH TIME ZONE DEFAULT now() NOT NULL,
        CONSTRAINT pk_bulk_action_logs PRIMARY KEY (id),
        CONSTRAINT ck_bulk_action_logs_status_valid
            CHECK (status IN ('success', 'failed', 'skipped')),
        CONSTRAINT fk_bulk_action_logs_bulk_action_id_bulk_actions
            FOREIGN KEY(bulk_action_id) REFERENCES bulk_actions (id) ON DELETE CASCADE,
        CONSTRAINT fk_bulk_action_logs_batch_id_bulk_action_batches
            FOREIGN KEY(batch_id) REFERENCES bulk_action_batches (id) ON DELETE CASCADE
    )
    """,
    """
    CREATE TABLE bulk_action_dedup (
        bulk_action_id UUID NOT NULL,
        dedup_key VARCHAR(512) NOT NULL,
        CONSTRAINT pk_bulk_action_dedup PRIMARY KEY (bulk_action_id, dedup_key),
        CONSTRAINT fk_bulk_action_dedup_bulk_action_id_bulk_actions
            FOREIGN KEY(bulk_action_id) REFERENCES bulk_actions (id) ON DELETE CASCADE
    )
    """,
]

INDEXES = [
    "CREATE INDEX ix_accounts_created_at ON accounts (created_at)",
    # (account_id, id) is what makes keyset batch planning an index-only scan.
    "CREATE INDEX ix_contacts_account_id_id ON contacts (account_id, id)",
    "CREATE INDEX ix_contacts_account_status ON contacts (account_id, status)",
    "CREATE INDEX ix_contacts_account_email_lower ON contacts (account_id, lower(email))",
    "CREATE INDEX ix_contacts_created_at ON contacts (created_at)",
    "CREATE INDEX ix_companies_account_id_id ON companies (account_id, id)",
    "CREATE INDEX ix_companies_account_status ON companies (account_id, status)",
    "CREATE INDEX ix_companies_account_domain_lower ON companies (account_id, lower(domain))",
    "CREATE INDEX ix_companies_created_at ON companies (created_at)",
    "CREATE INDEX ix_bulk_actions_account_created ON bulk_actions (account_id, created_at)",
    "CREATE INDEX ix_bulk_actions_status ON bulk_actions (status)",
    "CREATE INDEX ix_bulk_actions_created_at ON bulk_actions (created_at)",
    "CREATE INDEX ix_batches_action_status ON bulk_action_batches (bulk_action_id, status)",
    "CREATE INDEX ix_bulk_action_batches_created_at ON bulk_action_batches (created_at)",
    # Serves /logs?status=... with one index scan; the trailing id gives stable
    # keyset pagination inside a status.
    "CREATE INDEX ix_logs_action_status_id ON bulk_action_logs (bulk_action_id, status, id)",
    "CREATE INDEX ix_logs_action_entity ON bulk_action_logs (bulk_action_id, entity_id)",
    "CREATE INDEX ix_bulk_action_logs_created_at ON bulk_action_logs (created_at)",
]

DROP_ORDER = [
    "bulk_action_dedup",
    "bulk_action_logs",
    "bulk_action_batches",
    "bulk_actions",
    "companies",
    "contacts",
    "accounts",
]


def upgrade() -> None:
    for statement in TABLES:
        op.execute(statement)
    for statement in INDEXES:
        op.execute(statement)


def downgrade() -> None:
    for table in DROP_ORDER:
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
