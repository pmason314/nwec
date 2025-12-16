"""Utilities for compiling PowerPoint reports from templates.

This module provides functions to:
- Copy and modify PowerPoint templates
- Replace text placeholders
- Replace images in slides
"""

import datetime
import shutil
from pathlib import Path

from pptx.presentation import Presentation
from pptx.slide import Slide
from pptx.util import Inches

from nwec.constants import REPORTS

REPORT_TEMPLATE_PATH = Path(__file__).parent / "200281 Analysis Template.pptx"


def copy_template(start_date: datetime.date, end_date: datetime.date) -> Path:
    """Copy the template file to a new location."""
    if start_date > end_date:
        raise ValueError("Start date must be before the end date")

    start_month = start_date.strftime("%B")
    end_month = end_date.strftime("%B")
    start_year = start_date.year
    end_year = end_date.year
    report_title = ""
    if start_year != end_year:
        report_title = f"200281 Analysis {start_month} {start_year} - {end_month} {end_year}"
    else:
        report_title = f"200281 Analysis {start_month} - {end_month} {start_year}"
    output_path = Path(REPORTS / f"{report_title}.pptx")
    Path.mkdir(REPORTS, exist_ok=True)
    shutil.copy(
        REPORT_TEMPLATE_PATH,
        output_path,
    )
    return output_path


def add_centered_image(presentation: Presentation, slide: Slide, image_path: Path) -> None:
    """Insert an image onto the given slide and center it.

    The image at image_path is added to slide using fixed display dimensions
    (width=8.5 inches, height=5 inches). The function computes the left and top
    offsets from the presentation's slide dimensions to horizontally and vertically
    center the image. A small upward offset of 0.5 inches is applied to the
    vertical placement to allow for visual balance or top slide content.

        presentation (Presentation): python-pptx Presentation object used to obtain
            slide dimensions (presentation.slide_width and presentation.slide_height).
        slide (Slide): Target Slide object where the image will be inserted.
        image_path (Path): Path-like object pointing to the image file to insert.

    Raises:
        FileNotFoundError: If image_path does not exist.
        TypeError: If provided presentation or slide are not compatible with python-pptx.
        Exception: Any underlying exceptions raised by python-pptx when adding or sizing the image.

    """
    # Define image dimensions
    width = Inches(8.5)
    height = Inches(5)

    # Standard slide dimensions are 10" x 7.5"
    slide_width = presentation.slide_width
    slide_height = presentation.slide_height
    left = (slide_width - width) / 2  # type: ignore[reportOptionalOperand]
    top = (slide_height - height) / 2 - Inches(0.5)  # type: ignore[reportOptionalOperand]

    slide.shapes.add_picture(str(image_path), left, top, width=width, height=height)
