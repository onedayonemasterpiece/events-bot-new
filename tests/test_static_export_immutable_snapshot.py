from __future__ import annotations
import importlib.util
from pathlib import Path
import sqlite3
import os
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location('immutable_exporter', ROOT / 'site/scripts/export-production-preview-data.py')
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)

class ImmutableSnapshotTest(unittest.TestCase):
    def test_wal_snapshot_on_read_only_mount(self):
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            db = folder / 'snapshot #1.sqlite'
            con = sqlite3.connect(db)
            con.execute('pragma journal_mode=wal')
            con.execute('create table event(id integer)')
            con.execute('insert into event values(5370)')
            con.commit(); con.close()
            self.assertEqual(db.read_bytes()[18:20], bytes([2,2]))
            db.chmod(0o444); folder.chmod(0o555)
            try:
                if os.geteuid() != 0:  # root bypasses POSIX directory write bits
                    with self.assertRaises(sqlite3.OperationalError):
                        with EXPORTER.open_preview_database(db) as ordinary:
                            ordinary.execute("pragma table_info('event')").fetchall()
                with EXPORTER.open_preview_database(db, immutable=True) as readonly:
                    self.assertEqual(readonly.execute('select id from event').fetchone()[0],5370)
                    with self.assertRaises(sqlite3.OperationalError): readonly.execute('insert into event values(1)')
                self.assertFalse(Path(f'{db}-wal').exists())
                self.assertFalse(Path(f'{db}-shm').exists())
            finally:
                folder.chmod(0o755); db.chmod(0o644)

    def test_nonempty_wal_is_not_silently_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            db=Path(directory)/'snapshot.sqlite'
            Path(f'{db}-wal').write_bytes(b'uncheckpointed-data')
            with self.assertRaisesRegex(ValueError,'self-contained'):
                EXPORTER.open_preview_database(db, immutable=True)

    def test_kernel_only_requests_immutable_after_snapshot_validation(self):
        source=(ROOT/'kaggle/StaticSiteBuilder/static_site_builder.py').read_text()
        start=source.index('def export_preview_data_if_configured(')
        self.assertLess(source.index('validate_snapshot_input(db_path, config)',start),source.index("'--db-immutable'",start))

if __name__=='__main__':unittest.main()
