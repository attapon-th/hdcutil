#!/usr/bin/env python


from datetime import datetime, timedelta
import click
import sys
import os
from hdcutil import build_process
from glob import glob
from dacutil import worker


@click.group()
def cli():
    pass


@click.command()
@click.argument("files", nargs=-1)
@click.option("--directory", "-d", default="./output", help="Directory output")
@click.option("--template", "-t", default="by_hospcode", help="Template name")
def build(files, directory: str, template: str = "by_hospcode"):
    filenames = []
    for filename in files:
        # if our shell does not do filename globbing
        expanded = list(glob(filename))
        if len(expanded) == 0 and "*" not in filename:
            raise (click.BadParameter("{}: file not found".format(filename)))
        filenames.extend(expanded)
    if len(filenames) == 0:
        print("Error: no files found", file=sys.stderr)
        sys.exit(1)
    os.makedirs(directory, exist_ok=True)
    for filename in filenames:
        if filename.endswith(".ipynb") is False:
            print(f"not support file: {filename}", file=sys.stderr)
            continue
        s: str = build_process.build(filename=filename, template_name=template)
        name: str = os.path.basename(filename)
        if s != "":
            path: str = os.path.join(directory, name.split(".ipynb")[0] + ".py")
            with open(path, "w+") as f:
                f.write(s)
            print(f"Success: build process success, file: {path}", file=sys.stderr)
        else:
            print(f"Error: build process failed, file: {name}", file=sys.stderr)
            sys.exit(1)


@click.command("convert")
@click.argument("files", nargs=-1)
@click.option("--directory", "-d", default="./toutput", help="Directory output")
@click.option("--clear", is_flag=True, help="Clear directory output")
def convert(files, directory: str, clear: bool = False):
    # print(file_glob)
    filenames = []
    for filename in files:
        # if our shell does not do filename globbing
        expanded = list(glob(filename))
        if len(expanded) == 0 and "*" not in filename:
            raise (click.BadParameter("{}: file not found".format(filename)))
        filenames.extend(expanded)
    if len(filenames) == 0:
        print("Error: no files found", file=sys.stderr)
        sys.exit(1)

    for filename in filenames:
        if filename.endswith(".ipynb") is False:
            print(f"not support file: {filename}", file=sys.stderr)
            continue
        jpy: dict = build_process.read_ipynb(filename)
        name: str = filename.split(".ipynb")[0]
        name = os.path.basename(name)
        os.makedirs(directory, exist_ok=True)
        path: str = os.path.join(directory, name + ".py")
        with open(path, "w+") as f:
            for i, v in enumerate(jpy["cells"]):
                if v["cell_type"] == "code":
                    sources = v["source"]
                    for source in sources:
                        if source.startswith("%"):
                            source = "# " + source
                        f.write(source)
                f.write("\n")
            print(f"Success: build process success, file: {path}", file=sys.stderr)


@click.command("run")
@click.argument("files", nargs=-1)
@click.option("workers", "-w", default=1, help="Number of workers")
def run(files, workers: int = 1):
    filenames = []
    for filename in files:
        # if our shell does not do filename globbing
        expanded = list(glob(filename))
        if len(expanded) == 0 and "*" not in filename:
            raise (click.BadParameter("{}: file not found".format(filename)))
        filenames.extend(expanded)
    if len(filenames) == 0:
        print("Error: no files found", file=sys.stderr)
        sys.exit(1)
    if workers == 1:
        for file in filenames:
            executable_py(file)
    else:
        args = []
        for file in filenames:
            args.append({"filename": file})
        worker(workers, executable_py, args)

    sys.exit(0)


def executable_py(filename: str):
    dt: datetime = datetime.now()
    print(f"Starting Filename: {filename}", file=sys.stderr)
    print(f"{sys.executable} {filename}", file=sys.stderr)
    exit_code: int = os.system(f"{sys.executable} {filename}")

    deltatime: timedelta = datetime.now() - dt
    if exit_code == 0:
        print(f"Done. Filename: {filename}, Dulation: {deltatime}", file=sys.stderr)
    else:
        print(
            f"Failed[{exit_code}] Filename: {filename}, Dulation: {deltatime}",
            file=sys.stderr,
        )


cli.add_command(build)
cli.add_command(run)
cli.add_command(convert)
if __name__ == "__main__":
    cli()
