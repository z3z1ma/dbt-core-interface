from dbt_core_interface import DbtProject


if __name__ == "__main__":
    project = DbtProject()
    for i in project.list("source:showpad+"):
        print(i)
    print(bool(project))
