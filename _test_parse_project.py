from dbt_core_interface import DbtProject


if __name__ == "__main__":
    project = DbtProject()
    print(project.list("*"))
