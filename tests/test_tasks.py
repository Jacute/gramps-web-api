#
# Gramps Web API - A RESTful API for the Gramps genealogy program
#
# Copyright (C) 2026      David Straub
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
#

"""Tests for celery tasks."""

import shutil
import os
from unittest.mock import patch
import pytest

from gramps.gen.db import DbWriteBase
from gramps.gen.lib.person import Person
from gramps.gen.lib.name import Name
from gramps.gen.lib.surname import Surname
from gramps.gen.db import DbTxn

from gramps_webapi.api.tasks import restore_db, run_task
from gramps_webapi.api.util import get_db_outside_request
from gramps.gen.errors import HandleError
from gramps_webapi.app import create_app
from gramps_webapi.dbmanager import WebDbManager


RESTORE_DB_TEST_PATH = "/tmp/restore.gramps"
TREE_NAME = "TEST_TREE"
USER_ID = 123
RESTORE_DB_TESTCASES = [
    {
        "test_name": "ok: one family",
        "extension": "gramps",
        "people_count": 3,
        "family_count": 1,
        "event_count": 2,
        "persons": [],
        "file_data": """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE database PUBLIC "-//Gramps//DTD Gramps XML 1.7.2//EN"
"http://gramps-project.org/xml/1.7.2/grampsxml.dtd">
<database xmlns="http://gramps-project.org/xml/1.7.2/">
  <header>
    <created date="2026-02-17" version="6.0.6"/>
    <researcher>
    </researcher>
  </header>
  <events>
    <event handle="_101c1e800e372b48c96267f2a278" change="1771255813" id="E0001">
      <type>Birth</type>
    </event>
    <event handle="_f33425d8-9a16-4123-a053-43716e6d6dfa" change="1771255813" id="E0000">
      <type>Birth</type>
    </event>
  </events>
  <people>
    <person handle="_101c1e800e4335f8e7ea64308e2f" change="1771255792" id="I0003">
      <gender>F</gender>
      <name type="Birth Name">
        <first>Мать</first>
        <surname>Бочкарева</surname>
      </name>
      <parentin hlink="_101c1e800e633f29d2258a9c043b"/>
    </person>
    <person handle="_101c1e800e6d12845d2ffdedbc5d" change="1771255813" id="I0000">
      <gender>M</gender>
      <name type="Birth Name">
        <first>Егор</first>
        <surname>Бочкарев</surname>
      </name>
      <eventref hlink="_101c1e800e372b48c96267f2a278" role="Primary"/>
      <childof hlink="_101c1e800e633f29d2258a9c043b"/>
      <noteref hlink="_101c1e800e7423a99aee6081a40b"/>
    </person>
    <person handle="_101c1e800e7f64cf2abeb9f07134" change="1771255787" id="I0005">
      <gender>M</gender>
      <name type="Birth Name">
        <first>Отец</first>
        <surname>Бочкарев</surname>
      </name>
      <parentin hlink="_101c1e800e633f29d2258a9c043b"/>
    </person>
  </people>
  <families>
    <family handle="_101c1e800e633f29d2258a9c043b" change="1771255778" id="F0001">
      <rel type="Unknown"/>
      <father hlink="_101c1e800e7f64cf2abeb9f07134"/>
      <mother hlink="_101c1e800e4335f8e7ea64308e2f"/>
      <childref hlink="_101c1e800e6d12845d2ffdedbc5d"/>
    </family>
  </families>
  <objects>
    <object handle="_101c209d5493ea574d21790fff" change="1771299231" id="O0000">
      <file src="061775456dbad6ba5b4e8f726bc0379d.pdf" mime="application/pdf" checksum="061775456dbad6ba5b4e8f726bc0379d"/>
    </object>
    <object handle="_101c209fa2d875938481da263bb6" change="1771299246" id="O0001">
      <file src="bbcac69409dbd1f89b1da9013f778ecc.png" mime="image/png" checksum="bbcac69409dbd1f89b1da9013f778ecc"/>
    </object>
    <object handle="_101c20a412b71bc07dbf058e3a72" change="1771299275" id="O0002">
      <file src="a127a90bef56ae59f1ce37428062e6d6.png" mime="image/png" checksum="a127a90bef56ae59f1ce37428062e6d6"/>
    </object>
  </objects>
  <notes>
    <note handle="_101c1e800e7423a99aee6081a40b" change="1771255807" id="N0001" type="General">
      <text>вфывыфф</text>
    </note>
    <note handle="_9b21567a-c5dc-4087-95f5-b94e1641bc83" change="1771255807" id="N0000" type="General">
      <text>вфывыфф</text>
    </note>
  </notes>
</database>""",
    },
    {
        "test_name": "ok: two family",
        "extension": "gramps",
        "people_count": 6,
        "family_count": 2,
        "event_count": 4,
        "file_data": """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE database PUBLIC "-//Gramps//DTD Gramps XML 1.7.2//EN"
"http://gramps-project.org/xml/1.7.2/grampsxml.dtd">
<database xmlns="http://gramps-project.org/xml/1.7.2/">
  <header>
    <created date="2026-02-18" version="6.0.6"/>
    <researcher>
    </researcher>
  </header>
  <events>
    <event handle="_101c1e800e372b48c96267f2a278" change="1771255813" id="E0001">
      <type>Birth</type>
    </event>
    <event handle="_8013d5ac-41f1-4b00-8da4-50d37e0c7205" change="1771417888" id="E0002">
      <type>Birth</type>
      <dateval val="1970-09-08"/>
    </event>
    <event handle="_f33425d8-9a16-4123-a053-43716e6d6dfa" change="1771255813" id="E0000">
      <type>Birth</type>
    </event>
    <event handle="_fe5290e4-5632-4d98-8ecd-dcee578d054e" change="1771417904" id="E0003">
      <type>Birth</type>
      <dateval val="1980"/>
    </event>
  </events>
  <people>
    <person handle="_3590d473-aa5f-421f-b4df-4afe2b096fdd" change="1771255792" id="I0002">
      <gender>F</gender>
      <name type="Birth Name">
        <first>Мать</first>
        <surname>Бочкарева</surname>
      </name>
      <parentin hlink="_101c06b6ed217888019d5cd952fa"/>
    </person>
    <person handle="_101c1e800e6d12845d2ffdedbc5d" change="1771255813" id="I0000">
      <gender>M</gender>
      <name type="Birth Name">
        <first>Егор</first>
        <surname>Бочкарев</surname>
      </name>
      <eventref hlink="_f33425d8-9a16-4123-a053-43716e6d6dfa" role="Primary"/>
      <childof hlink="_101c06b6ed217888019d5cd952fa"/>
      <noteref hlink="_9b21567a-c5dc-4087-95f5-b94e1641bc83"/>
    </person>
    <person handle="_5bbcba1c-086b-436a-afd6-675c53582e12" change="1771255787" id="I0001">
      <gender>M</gender>
      <name type="Birth Name">
        <first>Отец</first>
        <surname>Бочкарев</surname>
      </name>
      <parentin hlink="_101c06b6ed217888019d5cd952fa"/>
    </person>
    <person handle="_6815ba88-53df-43a3-8819-eaaf6f9de960" change="1771417919" id="I0008">
      <gender>M</gender>
      <name type="Birth Name">
        <first>Отец</first>
        <surname>Зубенко</surname>
      </name>
      <eventref hlink="_fe5290e4-5632-4d98-8ecd-dcee578d054e" role="Primary"/>
      <parentin hlink="_101c675bcbf02090f6f9eb62323"/>
    </person>
    <person handle="_964f4592-d57e-4786-85de-dca6428387e0" change="1771417919" id="I0007">
      <gender>F</gender>
      <name type="Birth Name">
        <first>Мать</first>
        <surname>Зубенко</surname>
      </name>
      <eventref hlink="_8013d5ac-41f1-4b00-8da4-50d37e0c7205" role="Primary"/>
      <parentin hlink="_101c675bcbf02090f6f9eb62323"/>
    </person>
    <person handle="_debd70f9-ea78-4c7c-8e8d-247ea1582dfc" change="1771417919" id="I0006">
      <gender>M</gender>
      <name type="Birth Name">
        <first>Михаил</first>
        <surname>Зубенко</surname>
      </name>
      <childof hlink="_101c675bcbf02090f6f9eb62323"/>
    </person>
  </people>
  <families>
    <family handle="_101c06b6ed217888019d5cd952fa" change="1771255778" id="F0000">
      <rel type="Unknown"/>
      <father hlink="_5bbcba1c-086b-436a-afd6-675c53582e12"/>
      <mother hlink="_3590d473-aa5f-421f-b4df-4afe2b096fdd"/>
      <childref hlink="_101c1e800e6d12845d2ffdedbc5d"/>
    </family>
    <family handle="_101c675bcbf02090f6f9eb62323" change="1771417919" id="F0002">
      <rel type="Unmarried"/>
      <father hlink="_6815ba88-53df-43a3-8819-eaaf6f9de960"/>
      <mother hlink="_964f4592-d57e-4786-85de-dca6428387e0"/>
      <childref hlink="_debd70f9-ea78-4c7c-8e8d-247ea1582dfc"/>
    </family>
  </families>
  <objects>
    <object handle="_101c209d5493ea574d21790fff" change="1771299231" id="O0000">
      <file src="061775456dbad6ba5b4e8f726bc0379d.pdf" mime="application/pdf" checksum="061775456dbad6ba5b4e8f726bc0379d"/>
    </object>
    <object handle="_101c209fa2d875938481da263bb6" change="1771299246" id="O0001">
      <file src="bbcac69409dbd1f89b1da9013f778ecc.png" mime="image/png" checksum="bbcac69409dbd1f89b1da9013f778ecc"/>
    </object>
    <object handle="_101c20a412b71bc07dbf058e3a72" change="1771299275" id="O0002">
      <file src="a127a90bef56ae59f1ce37428062e6d6.png" mime="image/png" checksum="a127a90bef56ae59f1ce37428062e6d6"/>
    </object>
  </objects>
  <notes>
    <note handle="_101c1e800e7423a99aee6081a40b" change="1771255807" id="N0001" type="General">
      <text>вфывыфф</text>
    </note>
    <note handle="_9b21567a-c5dc-4087-95f5-b94e1641bc83" change="1771255807" id="N0000" type="General">
      <text>вфывыфф</text>
    </note>
  </notes>
</database>""",
    },
]
BROKEN_LINK_TEST_CASE = {
    "test_name": "broken link",
    "extension": "gramps",
    "persons": [],
    "file_data": """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE database PUBLIC "-//Gramps//DTD Gramps XML 1.7.2//EN"
"http://gramps-project.org/xml/1.7.2/grampsxml.dtd">
<database xmlns="http://gramps-project.org/xml/1.7.2/">
  <header>
    <created date="2026-02-17" version="6.0.6"/>
    <researcher>
    </researcher>
  </header>
  <events>

  </events>
  <people>
    <person handle="_101c1e800e4335f8e7ea64308e2f" change="1771255792" id="I0003">
      <gender>F</gender>
      <name type="Birth Name">
        <first>Мать</first>
        <surname>Бочкарева</surname>
      </name>
      <parentin hlink="_101c1e800e633f29d2258a9c043b"/>
    </person>
    <person handle="_101c1e800e6d12845d2ffdedbc5d" change="1771255813" id="I0000">
      <gender>M</gender>
      <name type="Birth Name">
        <first>Егор</first>
        <surname>Бочкарев</surname>
      </name>
      <eventref hlink="_101c1e800e372b48c96267f2a278" role="Primary"/>
      <childof hlink="_101c1e800e633f29d2258a9c043b"/>
      <noteref hlink="_101c1e800e7423a99aee6081a40b"/>
    </person>
    <person handle="_101c1e800e7f64cf2abeb9f07134" change="1771255787" id="I0005">
      <gender>M</gender>
      <name type="Birth Name">
        <first>Отец</first>
        <surname>Бочкарев</surname>
      </name>
      <parentin hlink="_101c1e800e633f29d2258a9c043b"/>
    </person>
  </people>
  <families>
    <family handle="_101c1e800e633f29d2258a9c043b" change="1771255778" id="F0001">
      <rel type="Unknown"/>
      <father hlink="_101c1e800e7f64cf2abeb9f07134"/>
      <mother hlink="_101c1e800e4335f8e7ea64308e2f"/>
      <childref hlink="_101c1e800e6d12845d2ffdedbc5d"/>
    </family>
  </families>
  <objects>
    <object handle="_101c209d5493ea574d21790fff" change="1771299231" id="O0000">
      <file src="061775456dbad6ba5b4e8f726bc0379d.pdf" mime="application/pdf" checksum="061775456dbad6ba5b4e8f726bc0379d"/>
    </object>
    <object handle="_101c209fa2d875938481da263bb6" change="1771299246" id="O0001">
      <file src="bbcac69409dbd1f89b1da9013f778ecc.png" mime="image/png" checksum="bbcac69409dbd1f89b1da9013f778ecc"/>
    </object>
    <object handle="_101c20a412b71bc07dbf058e3a72" change="1771299275" id="O0002">
      <file src="a127a90bef56ae59f1ce37428062e6d6.png" mime="image/png" checksum="a127a90bef56ae59f1ce37428062e6d6"/>
    </object>
  </objects>
  <notes>
    <note handle="_101c1e800e7423a99aee6081a40b" change="1771255807" id="N0001" type="General">
      <text>вфывыфф</text>
    </note>
    <note handle="_9b21567a-c5dc-4087-95f5-b94e1641bc83" change="1771255807" id="N0000" type="General">
      <text>вфывыфф</text>
    </note>
  </notes>
</database>""",
}
DB_PATH = "/tmp/gramps"


@pytest.fixture
def flask_app():
    """Fixture for initializing flask app."""
    os.makedirs(DB_PATH, exist_ok=True)
    os.environ["GRAMPS_DATABASE_PATH"] = DB_PATH
    app = create_app(
        {
            "TREE": TREE_NAME,
            "CELERY_CONFIG": {},
        }
    )
    return app


@pytest.fixture
def db(flask_app):  # pylint: disable=redefined-outer-name
    """Fixture for creating db."""
    WebDbManager(
        name=TREE_NAME,
        dirname=TREE_NAME,
        create_if_missing=True,
        create_backend="sqlite",
    )
    with (
        flask_app.app_context(),
        # mocks for get_db_handle
        patch("gramps_webapi.api.util.has_permissions", return_value=True),
        patch(
            "gramps_webapi.api.util.get_tree_from_jwt_or_fail",
            return_value=TREE_NAME,
        ),
        patch("gramps_webapi.api.util.get_jwt_identity", return_value=USER_ID),
    ):
        yield get_db_outside_request(
            tree=TREE_NAME,
            user_id=USER_ID,
            view_private=True,
            readonly=False,
        )
    if os.path.isdir(DB_PATH):
        shutil.rmtree(DB_PATH)


# @pytest.mark.parametrize("case", RESTORE_DB_TESTCASES)
# def test_restore_db_empty(case, db):  # pylint: disable=redefined-outer-name
#     """Restore db from backup over db without any data."""
#     with open(RESTORE_DB_TEST_PATH, mode="w", encoding="utf-8") as f:
#         f.write(case["file_data"])
#     run_task(
#         restore_db,
#         tree=TREE_NAME,
#         user_id=USER_ID,
#         file_name=RESTORE_DB_TEST_PATH,
#         extension="gramps",
#     )
#     db = get_db_handle(readonly=False)
#     assert db.get_number_of_people() == case["people_count"]
#     assert db.get_number_of_families() == case["family_count"]
#     assert db.get_number_of_events() == case["event_count"]


@pytest.mark.parametrize("case", RESTORE_DB_TESTCASES)
def test_restore_db_with_person_in_backup(
    case, db: DbWriteBase
):  # pylint: disable=redefined-outer-name
    """Restore db from backup over db with person from backup db.
    Tests that person was updated.
    """
    with open(RESTORE_DB_TEST_PATH, mode="w", encoding="utf-8") as f:
        f.write(case["file_data"])

    person = Person()
    person.handle = "_101c1e800e6d12845d2ffdedbc5d"
    person.gramps_id = "I0000"
    person.gender = Person.UNKNOWN
    name = Name()
    name.first_name = "Егор"
    surname = Surname()
    surname.surname = "Бочкарев"
    name.surname_list = [surname]
    person.primary_name = name

    db = get_db_outside_request(
        tree=TREE_NAME,
        user_id=USER_ID,
        view_private=True,
        readonly=False,
    )
    with DbTxn("Add test objects", db) as trans:
        db.add_person(person, trans)

    run_task(
        restore_db,
        tree=TREE_NAME,
        user_id=USER_ID,
        file_name=RESTORE_DB_TEST_PATH,
        extension="gramps",
    )

    # check count of objects
    assert db.get_number_of_people() == case["people_count"]
    assert db.get_number_of_families() == case["family_count"]
    assert db.get_number_of_events() == case["event_count"]
    # check already exists person
    new_person = db.get_person_from_gramps_id(person.gramps_id)
    assert new_person.gender == Person.MALE


@pytest.mark.parametrize("case", RESTORE_DB_TESTCASES)
def test_restore_db_with_person_not_in_backup(
    case, db: DbWriteBase
):  # pylint: disable=redefined-outer-name
    """Restore db from backup over db with person which isn't present in existing db.
    Tests that person was deleted after backup.
    """
    with open(RESTORE_DB_TEST_PATH, mode="w", encoding="utf-8") as f:
        f.write(case["file_data"])

    person = Person()
    person.gramps_id = "I0100"
    person.handle = "_101c1e800e6d12145d2ffdedbc5d"
    person.gender = Person.UNKNOWN
    name = Name()
    name.first_name = "Егор"
    surname = Surname()
    surname.surname = "Бочкарев"
    name.surname_list = [surname]
    person.primary_name = name

    with DbTxn("Add test objects", db) as trans:
        db.add_person(person, trans)

    run_task(
        restore_db,
        tree=TREE_NAME,
        user_id=USER_ID,
        file_name=RESTORE_DB_TEST_PATH,
        extension="gramps",
    )
    assert db.get_number_of_people() == case["people_count"]
    assert db.get_number_of_families() == case["family_count"]
    assert db.get_number_of_events() == case["event_count"]
    assert db.get_person_from_gramps_id(person.gramps_id) is None


@pytest.mark.parametrize("case", RESTORE_DB_TESTCASES)
def test_restore_db_with_broken_links(
    case, db: DbWriteBase
):  # pylint: disable=redefined-outer-name
    """Restore db with links person to events, but without events."""
    with open(RESTORE_DB_TEST_PATH, mode="w", encoding="utf-8") as f:
        f.write(case["file_data"])

    run_task(
        restore_db,
        tree=TREE_NAME,
        user_id=USER_ID,
        file_name=RESTORE_DB_TEST_PATH,
        extension="gramps",
    )
    broken_handle = "_101c1e800e372b48c96267f2a278"
    person_with_broken_link = "I0000"
    with pytest.raises(HandleError):
        db.get_event_from_handle(broken_handle)
    person: Person = db.get_person_from_gramps_id(person_with_broken_link)
    assert len(person.event_ref_list) == 0
