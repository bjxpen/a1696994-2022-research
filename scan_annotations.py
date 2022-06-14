"""ast module-based parser to count annotated tokens from python code.

scan_annotations.py
author: Jiaxing Peng

Most of the time you would only touch the class `AnnotationScanner`.
Usage examples can be seen from _main() and _main2().
"""

from pathlib import Path
from typing import Iterable, Union, List, Tuple, Optional, Generator
from dataclasses import asdict, fields, dataclass

import ast
import charset_normalizer

from natsort import humansorted
from astunparse import unparse


class AnnotationItemBase:
    """Base class for any annotatable tokens

    """
    is_annotated: bool
    annotated_text: Optional[str]
    code: str
    line_range: Tuple[int, Optional[int]]

    def __init__(self, node: ast.AST):
        self.line_range = (node.lineno, node.end_lineno)

    def _unparse_ast(self, node: ast.AST) -> str:
        """
        Turns ast into code
        """
        return unparse(node).strip()


class Assignment(AnnotationItemBase):
    """Assignment token

    Example code:
        foo = 1

    Same attributes as AnnotationItemBase
    """
    def __init__(self, node: Union[ast.AnnAssign, ast.Assign]):
        super().__init__(node)
        if isinstance(node, ast.AnnAssign):
            self.is_annotated = True
            self.annotated_text = self._unparse_ast(node.annotation)
        elif isinstance(node, ast.Assign):
            self.is_annotated = False
            self.annotated_text = None
        else:
            raise TypeError("Expected node type ast.AnnAssign or ast.Assign, "
                f"got {type(node).__name__}")
        self.code = self._unparse_ast(node)


class FunctionParameter(AnnotationItemBase):
    """Function parameter token

    Example code:
        def foo(param):

    Same attributes as AnnotationItemBase
    """
    def __init__(self, node: ast.arg):
        super().__init__(node)
        if node.annotation is None:
            self.annotated_text = None
            self.is_annotated = False
        else:
            self.annotated_text = self._unparse_ast(node.annotation)
            self.is_annotated = True
        self.code = self._unparse_ast(node)


class FunctionReturn(AnnotationItemBase):
    """Function header return annotation token

    Example code:
        def foo() -> str:

    Same attributes as AnnotationItemBase
    """
    def __init__(self, node: Union[ast.FunctionDef, ast.AsyncFunctionDef]):
        super().__init__(node)
        if node.returns is None:
            self.annotated_text = None
            self.is_annotated = False
        else:
            self.annotated_text = self._unparse_ast(node.returns)
            self.is_annotated = True
        bodyless_node = type(node)(node.name, node.args,
            ast.Expr(ast.Constant(...)), node.decorator_list,
            node.returns, node.type_comment)
        self.code = self._unparse_ast(bodyless_node)


@dataclass(frozen=True)
class AnnotationScanResultBase:
    """Internal class for code annotation scan result

    Contains all annotatable token instances
    """
    assignments: List[Assignment]
    func_params: List[FunctionParameter]
    func_returns: List[FunctionReturn]


@dataclass
class AnnotationScanStatsBase:
    """Statistics-only dataclass for code annotation scan result

    """
    total_assign_count: int
    annotated_assign_count: int
    unannotated_assign_count: int
    total_func_params_count: int
    annotated_func_params_count: int
    unannotated_func_params_count: int
    total_func_returns_count: int
    annotated_func_returns_count: int
    unannotated_func_returns_count: int


class AnnotationScanResult(AnnotationScanStatsBase, AnnotationScanResultBase):
    """Single-file code annotation scan result

    Attributes from both AnnotationScanStatsBase and AnnotationScanResultBase
    """
    def __init__(self, *args, **kwargs):
        AnnotationScanResultBase.__init__(self, *args, **kwargs)
        self._init_compute_stats()

    def get_stats_dict(self):
        """
        Return self but as dict
        """
        return asdict(self)

    def _init_compute_stats(self):
        assignments = self.assignments
        func_params = self.func_params
        func_returns = self.func_returns

        total_assignments = len(assignments)
        annotated_assignments = self._count_annotated(assignments)
        unannotated_assignments = total_assignments - annotated_assignments
        total_func_params = len(func_params)
        annotated_func_params = self._count_annotated(func_params)
        unannotated_func_params = total_func_params - annotated_func_params
        total_func_returns = len(func_returns)
        annotated_func_returns = self._count_annotated(func_returns)
        unannotated_func_returns = total_func_returns - annotated_func_returns
        AnnotationScanStatsBase.__init__(self,
            total_assignments, annotated_assignments, unannotated_assignments,
            total_func_params, annotated_func_params, unannotated_func_params,
            total_func_returns, annotated_func_returns, unannotated_func_returns,
        )

    def _count_annotated(self, anno_items: List[AnnotationItemBase]):
        return len([True for item in anno_items if item.is_annotated])


class AnnotationScanGroupResult(AnnotationScanStatsBase):
    """Multi-file/multipart code annotation scan result

    Tokens are not stored, so same attributes as AnnotationScanStatsBase
    """
    def __init__(self):
        AnnotationScanStatsBase.__init__(self, *([0] * 9))

    def _add_result(self, result: AnnotationScanStatsBase) -> None:
        """Modify stats result in-place by adding with another scan result

        """
        for field in fields(self):
            field_name = field.name
            val = getattr(self, field_name)
            new_val = val + getattr(result, field_name)
            setattr(self, field_name, new_val)

    def __add__(self, result: AnnotationScanStatsBase) -> "AnnotationScanGroupResult":
        """Add stats with stats from another scan result

        Returns:
            self
        """
        self._add_result(result)
        return self


class AnnotationScanner:
    """Python code annotation scanner

    Main class of the module
    """
    @classmethod
    def scan_text(cls, code: str) -> AnnotationScanResult:
        """Parses text and scans for annotatable Python code tokens

        """
        assignments: List[Assignment] = []
        func_params: List[FunctionParameter] = []
        func_returns: List[FunctionReturn] = []

        tree = ast.parse(code)
        for node in ast.walk(tree):
            if isinstance(node, (ast.AnnAssign, ast.Assign)):
                assignments.append(Assignment(node))
            if isinstance(node, ast.arg):
                func_params.append(FunctionParameter(node))
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                func_returns.append(FunctionReturn(node))

        return AnnotationScanResult(assignments, func_params, func_returns)

    # aliases
    scan_code = scan_text
    scan_str = scan_text
    scan = scan_text

    @classmethod
    def scan_file(cls, file_path: Union[Path, str]) -> AnnotationScanResult:
        """Guesses encoding of file, decodes, and scans for annotatable Python code tokens

        """
        if isinstance(file_path, str):
            file_path = Path(file_path)
        charset_res = charset_normalizer.from_path(file_path)
        text = str(charset_res.best())
        return cls.scan_text(text)

    @classmethod
    def scan_files(cls, paths: Iterable[Union[Path, str]]
    ) -> Generator[AnnotationScanGroupResult, str, Optional[AnnotationScanResult]]:
        """Scans for annotatable Python code in given files

        yields cummulative statistics summary, file path and per-file scan result
        """
        result = AnnotationScanGroupResult()
        for file_path in paths:
            file_result = cls.scan_file(file_path)
            result += file_result
            yield result, file_path, file_result

    @classmethod
    def scan_folder(cls,
        folder_path: Union[Path, str],
        glob: str = r"**/*.py"
    ) -> Optional[Generator[AnnotationScanGroupResult, str, Optional[AnnotationScanResult]]]:
        """Scans for annotatable Python code in given files, specified by folder and glob

        Natural sort is used for paths.

        Returns:
            None if no paths found
            Generator based on scan_files() if paths found
        """
        if isinstance(folder_path, str):
            folder_path = Path(folder_path)
        file_paths = humansorted(folder_path.glob(glob))

        if len(file_paths) == 0:
            return None

        return cls.scan_files(file_paths)


def _main1():
    """Example: scans sample code below

    """
    def _remove_toplevel_indent(code):
        import re
        match_obj = re.search("\n*([\t ]*)(?![\t ])", code)
        captured_groups = match_obj.groups()
        if len(captured_groups) == 0:
            return code
        indent = captured_groups[0]
        new_code = re.sub(f"^{indent}", "", code, flags=re.M)
        return new_code

    ans = AnnotationScanner()
    sample_code = """
        a = 1
        b:int = (
            2+2
        )

        def foo(bar) -> None:
            print(bar + 2)

        def foobar(fooo: str):
            return fooo + "abc"

        async def hmm() -> 9:
            ...

        foo(b)

        foobar("hi")

        if some_value:
            my_var = function() # type: Logger # real comments
        else:
            my_var = another_function() # random comment
    """
    res = ans.scan_text(_remove_toplevel_indent(sample_code))
    print(res.get_stats_dict())


def _main2():
    """Example: scans the cwd

    """
    ans = AnnotationScanner()
    gen = ans.scan_folder(".")
    if gen is not None:
        for summary, file, file_res in gen:
            print(file)
        print(summary)


if __name__ == "__main__":
    _main1()
    # _main2()
