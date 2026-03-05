"""Market category bucket resolution.

Tier 2 compliance: Local implementation, no unified-trading-library dependency.
Convention: {category}-store-{project_id} or {category}-store-{project_id}_test
"""


def get_bucket_for_category(
    category: str,
    project_id: str,
    test_mode: bool = False,
) -> str:
    """Get bucket name for market category.

    Convention: {category_lower}-store-{project_id}
    Test mode appends _test suffix.

    Args:
        category: CEFI, TRADFI, or DEFI
        project_id: GCP project ID
        test_mode: If True, append _test to bucket name

    Returns:
        Bucket name (e.g. cefi-store-my-project or cefi-store-my-project_test)
    """
    cat_lower = category.lower() if category else "cefi"
    bucket = f"{cat_lower}-store-{project_id}"
    if test_mode:
        bucket = f"{bucket}_test"
    return bucket
