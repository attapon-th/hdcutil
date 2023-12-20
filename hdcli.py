#!/usr/bin/env python


import click
import sys
import os
from hdcutil import build_process
from glob import glob


@click.group()
def cli():
    pass


@click.command()
@click.argument("file", type=click.File("rb"))
@click.option("--directory", "-d", default="./output", help="Directory output")
@click.option("--template", "-t", default="by_hospcode", help="Template name")
def build(file, directory: str, template: str = "by_hospcode"):
    filename = file.name
    file.close()
    s: str = build_process.build(filename=filename, template_name=template)
    name: str = os.path.basename(filename)
    if s != "":
        os.makedirs(directory, exist_ok=True)
        path: str = os.path.join(directory, name.split(".ipynb")[0] + ".py")
        with open(path, "w+") as f:
            f.write(s)
    else:
        print(f"Error: build process failed, file: {name}", file=sys.stderr)
        sys.exit(1)
    print(f"Success: build process success, file: {name}", file=sys.stderr)
    # output


@click.command("build-all")
@click.option("--directory", "-d", default="./output", help="Directory output")
@click.option("--clear", is_flag=True, help="Clear directory output")
@click.option("--template", "-t", default="by_hospcode", help="Template name")
@click.argument("dir")
def buildall(
    dir: str, directory: str, template: str = "by_hospcode", clear: bool = False
):
    if os.path.exists(dir):
        if clear:
            click.echo("Clear directory output")
            build_process.remove_all(directory)
        # ss = [sys.executable, sys.argv[0], "build" , "-d", directory]
        os.makedirs(directory, exist_ok=True)
        for root, dirs, files in os.walk(dir):
            for file in files:
                if file.endswith(".ipynb"):
                    # os.system(" ".join(ss + [os.path.join(root, file)]))
                    filename = os.path.join(root, file)
                    s: str = build_process.build(
                        filename=filename, template_name=template
                    )
                    name: str = os.path.basename(filename)
                    if s != "":
                        path: str = os.path.join(
                            directory, name.split(".ipynb")[0] + ".py"
                        )
                        with open(path, "w+") as f:
                            f.write(s)
                    else:
                        print(f"Error: {name}", file=sys.stderr)
                        continue
                        # sys.exit(1)
                    print(
                        f"Success: build process success, file: {name}", file=sys.stderr
                    )


@click.command("convert")
@click.argument("file_glob")
@click.option("--directory", "-d", default="./toutput", help="Directory output")
@click.option("--clear", is_flag=True, help="Clear directory output")
def convert(file_glob: str, directory: str, clear: bool = False):
    # print(file_glob)
    for filename in glob(file_glob):
        jpy: dict = build_process.read_ipynb(filename)
        name: str = filename.split(".ipynb")[0]
        name = os.path.basename(name)
        os.makedirs(directory, exist_ok=True)
        path: str = os.path.join(directory, name + ".py")
        with open(path, "w+") as f:
            for i, v in enumerate(jpy["cells"]):
                if v["cell_type"] == "code":
                    f.write("".join(v["source"]))
                f.write("\n")


@click.command("run")
@click.argument("file-glob")
def run(file_glob: str):
    files: list[str] = glob(file_glob)
    if len(files) == 0:
        print("file not found.", file=sys.stderr)
        sys.exit(1)
    for file in files:
        print(file)
        os.system(f"python {file}")
        print("Done.", file=sys.stderr)
    sys.exit(0)


cli.add_command(build)
cli.add_command(buildall)
cli.add_command(run)
cli.add_command(convert)
if __name__ == "__main__":
    cli()
