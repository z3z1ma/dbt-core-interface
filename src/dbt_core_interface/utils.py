from typing import TYPE_CHECKING, Tuple


if TYPE_CHECKING:
    from dbt.semver import VersionSpecifier


JINJA_CONTROL_SEQUENCES = ["{{", "}}", "{%", "%}", "{#", "#}"]


def has_jinja(query: str) -> bool:
    return any(seq in query for seq in JINJA_CONTROL_SEQUENCES)


def semvar_to_tuple(semvar: "VersionSpecifier") -> Tuple[int, int, int]:
    return (int(semvar.major) or 0, int(semvar.minor) or 0, int(semvar.patch) or 0)
