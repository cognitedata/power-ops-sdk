from cognite.client.utils._identifier import IdentifierSequence

from cognite.powerops._clients.powerops_client import get_powerops_client
from cognite.client.data_classes import SequenceUpdate, RelationshipList, Relationship, RelationshipUpdate
from cognite.powerops.resync._main import _load_transform
from tests.constants import ReSync, REPO_ROOT
from tests.utils import chdir


def main():
    new_dataset = "uc:000:powerops"
    with chdir(REPO_ROOT):
        power = get_powerops_client()
        cdf = power.cdf
        data_set_id = cdf.data_sets.retrieve(external_id=new_dataset).id
        if data_set_id is None:
            raise ValueError(f"Could not find data set {new_dataset}")
        loaded_models = _load_transform(
            "DayAhead",
            ReSync.demo,
            power.cdf.config.project,
            print,
            ["ProductionModel", "MarketModel", "CogShop1Asset"],
        )
        for model in loaded_models:
            sequences = model.sequences()
            existing = cdf.sequences.retrieve_multiple(
                external_ids=[s.external_id for s in sequences], ignore_unknown_ids=True
            )
            updates = []
            for seq in existing:
                if seq.data_set_id != data_set_id:
                    update = SequenceUpdate(id=seq.id)
                    update.data_set_id.set(data_set_id)
                    updates.append(update)
            if updates:
                cdf.sequences.update(updates)
                print(f"Updated sequences {updates} sequences for {model.model_name}")
            else:
                print(f"No sequences to update for {model.model_name}")
            if not hasattr(model, "relationships"):
                continue
            relationships = model.relationships()

            identifiers = IdentifierSequence.load(ids=None, external_ids=[r.external_id for r in relationships])
            existing = cdf.relationships._retrieve_multiple(
                list_cls=RelationshipList,
                resource_cls=Relationship,
                identifiers=identifiers,
                other_params={"ignoreUnknownIds": True},
            )

            updates = []
            for rel in existing:
                if rel.data_set_id != data_set_id:
                    update = RelationshipUpdate(external_id=rel.external_id)
                    update.data_set_id.set(data_set_id)
                    updates.append(update)
            if updates:
                cdf.relationships.update(updates)
                print(f"Updated {len(updates)} relationships for {model.model_name}")
            else:
                print(f"No relationships to update for {model.model_name}")


if __name__ == "__main__":
    main()
