from abc import ABCMeta
from pathlib import Path
from typing import Union, Any
from dataclasses import dataclass

import io
import os
import sys
import subprocess
import re
import ast
import csv
import traceback

from natsort import humansorted, natsorted
from radon.visitors import ComplexityVisitor
from cognitive_complexity.api import get_cognitive_complexity_for_node

import charset_normalizer
import jc
import pygit2

from scan_annotations import AnnotationScanner, AnnotationScanGroupResult
from config import REPO_FIRST_SAMPLE_DIR_PATH, REPO_FIRST_SAMPLE_METRICS_FILE_PATH


def short_path(path, max_len, delim = " .. "):
    half_mlen = int(max_len/2)
    delim_len = len(delim)
    reduced_half_mlen = half_mlen - delim_len

    if len(path) <= max_len:
        return path.ljust(max_len)
    shortened_path = path[:half_mlen] + delim + path[-reduced_half_mlen]
    return shortened_path

class ScannerAbstractClass(metaclass=ABCMeta):
    @classmethod
    def scan_code(cls, code):
        raise NotImplementedError
        return {}

    @classmethod
    def scan_file(cls, file_path) -> Any:
        if isinstance(file_path, str):
            file_path = Path(file_path)
        charset_res = charset_normalizer.from_path(file_path)
        text = str(charset_res.best())
        return cls.scan_code(text)

    @classmethod
    def scan_files(cls, file_paths) -> dict:
        scan_summary = ScanSummary()
        cwd = os.getcwd()
        for file_path in file_paths:
            print_path  = str(Path(file_path).relative_to(cwd))
            print_path = short_path(print_path, 100)

            print(f"Scanning {print_path}", end="\r", flush=True)

            file_result = cls.scan_file(file_path)
            scan_summary += file_result
        
        return scan_summary

    @classmethod
    def scan_folder(cls, dir_path) -> dict:
        glob_pattern = "**/*.py"

        if isinstance(dir_path, str):
            dir_path = Path(dir_path)
        file_paths = humansorted(dir_path.glob(glob_pattern))

        if len(file_paths) == 0:
            return None

        return cls.scan_files(file_paths)

class RepositoryNameScanner(ScannerAbstractClass):
    @classmethod
    def scan_folder(cls, dir_path: Union[str, Path]) -> dict:
        dir_path = Path(dir_path)
        dir_name = dir_path.name
        if "@" not in dir_name:
            raise Exception(f"Directory {dir_name} (in \"{dir_path.absolute()}\") has wrong repo-clone format. "
                "All directories must be named in format \"repo-name@repo-owner\"")
        repo_name, repo_owner = dir_name.split("@", 1)
        full_name = f"{repo_owner}/{repo_name}"
        return dict(name = full_name)

class AnnotationScannerProxy(ScannerAbstractClass):
    @classmethod
    def scan_folder(cls, dir_path) -> AnnotationScanGroupResult:
        gen = AnnotationScanner.scan_folder(dir_path)

        if gen is None:
            return AnnotationScanGroupResult()
            
        cwd = os.getcwd()
        for summary, file_path, file_res in gen:
            print_path = str(Path(file_path).relative_to(cwd))
            print_path = short_path(print_path, 75)
            print(f"AnnotationScanner: scanning {print_path}", end="\r", flush=True)
        
        return summary

class LOCCommentScanner(ScannerAbstractClass):
    @dataclass
    class Result:
        file_count: int
        py_loc: int
        py_loc_ratio: float
        py_comment_loc: int
        py_comment_loc_ratio: float

    @classmethod
    def scan_folder(cls, dir_path) -> Result:
        output_stream = io.StringIO()
        args = ['pygount', "-s", "py", "-f", "summary", "-d", str(dir_path)]
        process = subprocess.Popen(args, stdout=subprocess.PIPE)
        print(f"Scanning LOC and comments in {Path(dir_path).relative_to(os.getcwd())}...", end="\r")
        while True:
            line = process.stdout.readline().decode()
            if not line: 
                break
            output_stream.write(line)
            sys.stdout.write(line)
        output_text = output_stream.getvalue().strip()
        output_stream.close()
        result = cls._parse_stdout_summary(output_text)
        return result

    @classmethod
    def _parse_stdout_summary(cls, summary_text: str ) -> Result:
        summary_line = summary_text.strip().split("\n")[-2]
        columns = [col.strip() for col in summary_line.strip().split("|")]
        (_, _, file_count, _, py_loc, py_loc_percent, 
            py_comment_loc, py_comment_percent, _) = columns
        file_count = int(file_count)
        py_loc = int(py_loc)
        py_loc_ratio = float(py_loc_percent) / 100
        py_comment_loc = int(py_comment_loc)
        py_comment_loc_ratio = float(py_comment_percent) / 100
        result = cls.Result(file_count, py_loc, py_loc_ratio, py_comment_loc, py_comment_loc_ratio)
        return result

class PylintScanner(ScannerAbstractClass):
    LOC_REGEX = re.compile("[0-9]+(?=[\s]*lines have been analyzed)", flags=re.DOTALL)
    RAW_METRICS_TABLE_REGEX = re.compile("(?<=Raw metrics).*(?=Duplication)", flags=re.DOTALL)
    CATEGORY_MSG_TABLE_REGEX = re.compile("(?<=Messages by category).*(?=Messages)", flags=re.DOTALL)
    ALL_MSG_TABLE_REGEX = re.compile("(?<=Messages\n).*(?=\n{5})", flags=re.DOTALL)
    SCORE_REGEX = re.compile("(?<=Your code has been rated at )[0-9\.]+(?=\/10)", flags=re.DOTALL)

    @classmethod
    def scan_folder(cls, dir_path) -> dict:
        output_stream = io.StringIO()
        capture_line = False
        args = ["pylint", "--recursive=y", "-f=parseable", "-r=y", str(dir_path)]
        process = subprocess.Popen(args, stdout=subprocess.PIPE)
        print(f"Running pylint in {Path(dir_path).relative_to(os.getcwd())}...", end="\r")
        while True:
            line = process.stdout.readline().decode()
            if not line: 
                break
            if "lines have been analyzed" in line:
                capture_line = True
            if capture_line:
                output_stream.write(line)
            line = line.strip().split("\n")[-1]
            line = (line[:96] + ' .. ') if len(line) > 100 else line.ljust(100)
            print(line, end="\r", flush=True)
        output_text = output_stream.getvalue().strip()
        output_stream.close()
        result = cls._parse_stdout_summary(output_text)
        return result

    @classmethod
    def _parse_stdout_summary(cls, summary_text: str ) -> dict:
        result = {}

        raw_metrics_table = next(cls.RAW_METRICS_TABLE_REGEX.finditer(summary_text)).group(0).strip()
        raw_metrics = jc.parse("asciitable", raw_metrics_table)
        for row_dict in raw_metrics:
            prop_name = row_dict["type"]
            loc_count = row_dict["number"]
            percent = row_dict[""]
            result[f"py_{prop_name}_loc"] = int(loc_count)
            result[f"py_{prop_name}_loc_ratio"] = float(percent)/100
        
        category_msg_table = next(cls.CATEGORY_MSG_TABLE_REGEX.finditer(summary_text)).group(0).strip()
        category_messages = jc.parse("asciitable", category_msg_table)
        for row_dict in category_messages:
            message_type = row_dict["type"]
            type_count = row_dict["number"]
            result[f"pylint_{message_type}_msg"] = int(type_count)

        loc = next(cls.LOC_REGEX.finditer(summary_text)).group(0).strip()
        loc = int(loc)
        result["py_non_empty_loc"] = loc - result["py_empty_loc"]

        score = next(cls.SCORE_REGEX.finditer(summary_text)).group(0).strip()
        score = float(score)
        result["pylint_score"] = score
        return result

class CyclomaticComplexityScanner(ScannerAbstractClass):
    @classmethod
    def scan_code(cls, code: str):
        cc = ComplexityVisitor.from_code(code)
        complexity = cc.total_complexity
        return dict(cyclomatic_complexity = complexity)

class CognitiveComplexityScanner(ScannerAbstractClass):
    @classmethod
    def scan_code(cls, code: str):
        tree = ast.parse(code)
        complexity = get_cognitive_complexity_for_node(tree)
        return dict(cognitive_complexity=complexity)

class RepoBugScanner(ScannerAbstractClass):
    @classmethod
    def scan_folder(cls, dir_path) -> dict:
        repo = pygit2.Repository(dir_path)
        bug_score = 0
        for commit in repo.walk(repo.head.target):
            msg = commit.message
            if "bug" in msg or "fix" in msg:
                bug_score += 1

        return dict(bug_score=bug_score)

class ScanSummary(dict):
    def _add_result(self, result: Any):
        """Adds result in-place.
        
        """
        if isinstance(result, dict):
            for key, val in result.items():
                if key in self:
                    self[key] += val
                else:
                    self[key] = val
            return

        for prop_name in dir(result):
            if prop_name.startswith("_"):
                continue
            val = getattr(result, prop_name, 0)

            if not isinstance(val, (int, float)):
                continue

            if prop_name in self:
                self[prop_name] += val
            else:
                self[prop_name] = val

    def __add__(self, result: Any):
        """_add_result() but returns self too.
        
        """
        self._add_result(result)
        return self

def scan_folder(dir_path, scanners):
    scan_summary = ScanSummary()
    for scanner in scanners:
        result = scanner.scan_folder(dir_path)
        # print(result)
        scan_summary += result
    return scan_summary

def start_scan():
    repo_root_dir = Path(__file__).parent.joinpath(REPO_FIRST_SAMPLE_DIR_PATH)
    folders = [direntry.path for direntry in os.scandir(repo_root_dir) if direntry.is_dir()]
    folders = natsorted(folders, key=lambda path: Path(path).name)
    
    scanners = [
        RepositoryNameScanner,
        AnnotationScannerProxy,
        # LOCCommentScanner,
        PylintScanner,
        CyclomaticComplexityScanner,
        CognitiveComplexityScanner,
        RepoBugScanner
    ]

    results = []
    output_csv_path = Path(__file__).parent.joinpath(REPO_FIRST_SAMPLE_METRICS_FILE_PATH)
    csv_inited = False
    writer = None

    file_already_exists = output_csv_path.exists()
    # always just append new rows ,thus the 'a' (though create if not exists)
    with output_csv_path.open('a', newline='') as csv_file:
        for dir_path in folders:
            dir_path = Path(dir_path)
            if not dir_path.exists():
                continue

            print(f"Processing {dir_path}")
            try:
                result = scan_folder(dir_path, scanners)
            except:
                print(f"Failed processing {dir_path}")
                print(traceback.format_exc())
                continue

            if not csv_inited:
                writer = csv.DictWriter(csv_file, fieldnames = tuple(result.keys()))
                if not file_already_exists:
                    writer.writeheader()
                csv_inited = True

            writer.writerow(result)
            csv_file.flush()
            results.append(result)
    
if __name__ == "__main__":
    start_scan()
