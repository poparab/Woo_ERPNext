"""Production cleanup - removes all temporary and test files.

This script removes:
- Test utilities
- Migration test files
- Temporary optimization files
- Development documentation

Keeps only production-ready files.
"""
import os
import frappe


def cleanup_dev_files_cli():
    """Remove all development and test files, keep only production code.
    
    Usage:
        bench --site [site] execute \\
          jarz_woocommerce_integration.utils.cleanup_production.cleanup_dev_files_cli
    """
    print("\n" + "="*60)
    print("🧹 Production Cleanup")
    print("="*60)
    print("\nThis will remove development/test files:")
    print("  - utils/test_migration.py")
    print("  - utils/check_order_status.py")
    print("  - utils/migrate_hyper_optimized.py (failed version)")
    print("  - Development documentation")
    print("\nKeeps:")
    print("  ✓ utils/migrate_parallel.py (parallel workers)")
    print("  ✓ utils/migrate_ultra_optimized.py (proven fast)")
    print("  ✓ utils/add_sync_indexes.py (database indexes)")
    print("  ✓ All core services/")
    print("="*60)
    
    response = input("\nProceed with cleanup? (yes/no): ")
    if response.lower() != "yes":
        print("❌ Cleanup cancelled")
        return
    
    # Files to remove (relative to app root)
    files_to_remove = [
        "utils/test_migration.py",
        "utils/check_order_status.py",
        "utils/migrate_hyper_optimized.py",
        "SYNC_OPTIMIZATION_GUIDE.md",
        "OPTIMIZATION_COMPLETE.md",
        "OPTIMIZATION_DEPLOYED_TESTED.md",
    ]
    
    app_path = frappe.get_app_path("jarz_woocommerce_integration")
    parent_path = os.path.dirname(app_path)
    
    removed = []
    not_found = []
    
    for file_path in files_to_remove:
        full_path = os.path.join(parent_path, file_path)
        if os.path.exists(full_path):
            os.remove(full_path)
            removed.append(file_path)
            print(f"  ✓ Removed: {file_path}")
        else:
            not_found.append(file_path)
            print(f"  ⊘ Not found: {file_path}")
    
    print("\n" + "="*60)
    print("✅ Cleanup Complete!")
    print("="*60)
    print(f"Removed: {len(removed)} files")
    print(f"Not found: {len(not_found)} files")
    print("="*60 + "\n")


if __name__ == "__main__":
    cleanup_dev_files_cli()
