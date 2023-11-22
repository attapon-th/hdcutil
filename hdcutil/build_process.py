import json
import os
import shutil
from click import FileError


__IGNORED_PARAMETERS__ = [
    "budget_year",
    "budget_start_date",
    "budget_ended_date",
    "province_code",
    "hospcode",
    "ErrDataframeEmpty"
]

def build(filename: str,) -> str:
    jpy:dict = read_ipynb(filename)
    name:str = filename.split(".ipynb")[0]
    name = os.path.basename(name)
    data = dict(parameters=[], process=[])
    for i, v in  enumerate(jpy['cells']):
        if   v['cell_type'] == "code":
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
    template: list[str] = get_template_lines()
    pyscript:str = format_template(template, data)
    return pyscript
    


def get_template_lines() -> list[str]:
    base_dir: str = os.path.dirname(os.path.abspath(__file__))
    with open(base_dir +"/template/process_summary.py", "r") as f:
        return f.readlines()


def read_ipynb(filename:str) -> dict:
    if not filename.endswith(".ipynb"):
        raise FileError("Only .ipynb files are supported")
    with open(filename, "r") as f:
        return json.load(f)

def format_template(template: list[str], data: dict) -> str:
    s:str = ""
    for l in template:
        if  "__PYSCRIPT_PARAMETERS__" in l:
            params:list[str] = data["parameters"]
            p:str = "".join(params)
            l:str = p
            l += "\n"
        if "__PROCESSING_CODE__" in l:
            code:list[str] = data["process"]
            pos_index:int = l.index("__PROCESSING_CODE__")
            spaces:str = ""
            if pos_index > 0: 
                spaces:str =  l[:pos_index]
            l:str = spaces + spaces.join(code)
            l += "\n"
            
        s = s + l
    return s




def filter_parameter(parameters: list, name: str):
    params: list = []
    is_output_filename: bool = False
    for param in parameters:
        p:str = param.split("=")[0]
        p = p.split(":")[0]
        p = p.strip()
        if p in __IGNORED_PARAMETERS__:
            continue
        if "output_filename" == p:
            is_output_filename = True
            continue
        params.append(param)
    if is_output_filename == False:
        params.append(f"output_filename: str = '{name}'\n")
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
            print('Failed to delete %s. Reason: %s' % (file_path, e))