#!/usr/bin/env python


from email.policy import default
from http import client
import click
import sys
import os
from hdcutil import build_process

@click.group()
def cli():
    pass

@click.command()
@click.argument('file', type=click.File('rb'))
@click.option('--directory', "-d",  default='./output', help="Directory output")
def build(file, directory:str):
    filename = file.name
    file.close()
    s:str = build_process.build(filename=filename)
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
@click.option('--directory', "-d",  default='./output', help="Directory output")
@click.option('--clear',  is_flag=True , help="Clear directory output")
@click.argument('dir')
def buildall(dir:str , directory:str, clear: bool = False):
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
                    s:str = build_process.build(filename=filename)
                    name: str = os.path.basename(filename)
                    if s != "":
                        path: str = os.path.join(directory, name.split(".ipynb")[0] + ".py")
                        with open(path, "w+") as f:
                            f.write(s)
                    else:
                        print(f"Error: {name}", file=sys.stderr)
                        continue
                        # sys.exit(1)
                    print(f"Success: build process success, file: {name}", file=sys.stderr)



cli.add_command(build)
cli.add_command(buildall)
if __name__ == '__main__':
    cli()