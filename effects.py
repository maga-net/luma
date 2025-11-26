from PIL import Image
from typing import Iterator

def fade(start_image: Image.Image, end_image: Image.Image, steps: int = 10) -> Iterator[Image.Image]:
    """
    Generates a sequence of images that create a fade transition between a start
    and an end image.

    This function is a generator that yields each intermediate frame of the
    transition. It uses alpha blending to create the fade effect. If the source
    images are in '1' mode (monochrome), they are temporarily converted to
    grayscale for blending and then dithered back to '1' mode for each frame.

    :param start_image: The starting PIL Image object.
    :param end_image: The ending PIL Image object. Must be the same size and
        mode as the start_image.
    :param steps: The number of intermediate frames to generate for the
        transition. A higher number results in a smoother but slower
        transition. Defaults to 10.
    :type start_image: PIL.Image.Image
    :type end_image: PIL.Image.Image
    :type steps: int
    :raises ValueError: if the images are not the same size or mode, or if the
        image mode is not supported for blending ('1', 'L', 'RGB', 'RGBA').
    :yields: A PIL Image object for each step of the fade.
    :rtype: Iterator[PIL.Image.Image]

    Example:
        # Assuming 'device' is an initialized luma device in '1' mode
        from PIL import Image, ImageDraw
        from luma.core.effects import fade

        start = Image.new(device.mode, device.size)
        draw = ImageDraw.Draw(start)
        draw.rectangle(device.bounding_box, outline=\"white\", fill=\"black\")
        draw.text((10, 10), \"Start\", fill=\"white\")

        end = Image.new(device.mode, device.size)
        draw = ImageDraw.Draw(end)
        draw.rectangle(device.bounding_box, outline=\"white\", fill=\"black\")
        draw.text((40, 20), \"End\", fill=\"white\")

        # Display the fade animation
        for frame in fade(start, end, steps=25):
            device.display(frame)
            # A small delay can make the animation smoother on fast devices
            # import time; time.sleep(0.01)
    """
    if start_image.size != end_image.size:
        raise ValueError("Start and end images must be the same size.")
    if start_image.mode != end_image.mode:
        raise ValueError("Start and end images must be the same mode.")

    if steps <= 0:
        steps = 1

    original_mode = start_image.mode
    blend_start = start_image
    blend_end = end_image

    if original_mode == '1':
        # Convert to 'L' for blending
        blend_start = start_image.convert('L')
        blend_end = end_image.convert('L')
    elif original_mode not in ['L', 'RGB', 'RGBA']:
        raise ValueError(f"Unsupported image mode for fade effect: {original_mode}")

    for i in range(steps + 1):
        alpha = i / steps
        blended_image = Image.blend(blend_start, blend_end, alpha)

        if original_mode == '1':
            # Dither back to monochrome for display
            yield blended_image.convert('1')
        else:
            yield blended_image
