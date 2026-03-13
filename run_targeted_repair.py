"""Run targeted repair for missing child color/size attrs on FR marketplace."""
import asyncio
import json

from app.services.family_mapper.restructure import targeted_repair_missing_child_attrs


async def main() -> None:
    result = await targeted_repair_missing_child_attrs(
        family_id=1367,
        target_marketplace_id="A13V1IB3VIYZZH",  # FR
        dry_run=False,
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
