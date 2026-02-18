from gramps.gen.db.base import DbReadBase


def iter_all_objects(db: DbReadBase):
    OBJECT_TYPES = [
        "person",
        "family",
        "event",
        "place",
        "source",
        "citation",
        "repository",
        "media",
        "note",
        "tag",
    ]
    for obj_type in OBJECT_TYPES:
        iter_method = db.method(f"iter_{obj_type}_handles")
        get_method = db.method(f"get_{obj_type}_from_handle")
        for handle in iter_method():
            yield obj_type, get_method(handle)
