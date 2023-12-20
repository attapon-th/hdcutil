import json
import os
import shutil
from click import FileError


__IGNORED_PARAMETERS__ = [
    "budget_year",
    "b_year",
    "budget_start_date",
    "budget_ended_date",
    "b_date_start",
    "b_date_end",
    "province_code",
    "hospcode",
    "DataframeEmpty",
]


def build(filename: str, *, template_name: str = "by_hospcode") -> str:
    jpy: dict = read_ipynb(filename)
    name: str = filename.split(".ipynb")[0]
    name = os.path.basename(name)
    data = dict(parameters=[], process=[])
    for i, v in enumerate(jpy["cells"]):
        if v["cell_type"] == "code":
            if "tags" in v["metadata"]:
                tags = v["metadata"]["tags"]
                if "process" in tags:
                    # print(i, "process")
                    data["process"] += v["source"]
                elif True in [t in ["parameters", "param", "params"] for t in tags]:
                    # print(i, "parameters")
                    data["parameters"] += v["source"]
    if len(data["process"]) == 0:
        return ""
    # if len(data["parameters"]) == 0:

    data["parameters"] = filter_parameter(data["parameters"], name)
    template: list[str] = get_template_lines(template_name)
    pyscript: str = format_template(template, data)
    return pyscript


def get_template_lines(template_name: str = "by_hospcode") -> list[str]:
    base_dir: str = os.path.dirname(os.path.abspath(__file__))
    template_file: str = base_dir + f"/template/{template_name}.py"
    if os.path.exists(template_file) is False:
        template_file: str = base_dir + "/template/by_hospcode.py"
    with open(template_file, "r") as f:
        return f.readlines()


def read_ipynb(filename: str) -> dict:
    if not filename.endswith(".ipynb"):
        raise FileError("Only .ipynb files are supported")
    with open(filename, "r") as f:
        return json.load(f)


def format_template(template: list[str], data: dict) -> str:
    s: str = ""
    for line in template:
        if "__PYSCRIPT_PARAMETERS__" in line:
            line += "\n"
            pos_index: int = line.index("__PYSCRIPT_PARAMETERS__")
            spaces: str = ""
            if pos_index > 0:
                spaces: str = line[:pos_index]
            line += format_lines(data["parameters"], spaces)
            line += "\n"
        if "__PROCESSING_CODE__" in line:
            line += "\n"
            pos_index: int = line.index("__PROCESSING_CODE__")
            spaces: str = ""
            if pos_index > 0:
                spaces: str = line[:pos_index]
            line += format_lines(data["process"], spaces)
            line += "\n"

        s = s + line
    return s


def format_lines(list_line: list[str], spaces: str = "") -> str:
    strs: str = ""
    for line in list_line:
        strs += spaces + line
        if not line.endswith("\n"):
            strs += "\n"
    strs += "\n"
    return strs


def filter_parameter(parameters: list, name: str):
    params: list = []
    is_output_filename: bool = False
    for param in parameters:
        p: str = param.split("=")[0]
        p = p.split(":")[0]
        p = p.strip()
        if p in __IGNORED_PARAMETERS__:
            continue
        if "output_filename" == p:
            is_output_filename = True
            continue
        params.append(param)
    if is_output_filename is False:
        params.append(f"\noutput_filename: str = '{name}'\n")
    return params


def remove_all(folder: str):
    for filename in os.listdir(folder):
        file_path = os.path.join(folder, filename)
        try:
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.unlink(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)
        except Exception as e:
            print("Failed to delete %s. Reason: %s" % (file_path, e))
