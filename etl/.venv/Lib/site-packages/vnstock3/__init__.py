from .common.vnstock import Vnstock
from .common.data.data_explorer import Quote, Listing, Trading, Company, Finance, Screener
from .common.plot import chart_wrapper
import vnai
from IPython.display import display, Markdown
__all__ = ['Vnstock', 'Quote', 'Listing', 'Trading', 'Company', 'Finance']

vnai.setup()

import sys

def is_notebook():
    try:
        shell = get_ipython().__class__.__name__
        if shell == 'ZMQInteractiveShell':
            return True   # Jupyter notebook or qtconsole
        elif shell == 'TerminalInteractiveShell':
            return False  # Terminal running IPython
        else:
            return False  # Other type (?)
    except NameError:
        return False      # Probably standard Python interpreter

update_message = """
╔══════════════════════════════════════════════════════════════╗
║                                                              ║
║   🚀 Xin chào! Có tin vui cho bạn đây! 🎉                    ║
║                                                              ║
║   Bạn đang dùng phiên bản phần mềm 'vnstock3'.               ║
║   Hiện tại Vnstock đã hợp nhất tên gọi thư viện              ║
║   thành 'vnstock'. Hay quá phải không nào? 😃                ║
║                                                              ║
║   Hãy cập nhật ngay để trải nghiệm phiên bản mới nhất:       ║
║                                                              ║
║       pip install -U vnstock                                 ║
║                                                              ║
║   Muốn biết thêm chi tiết? Ghé thăm:                         ║
║   https://vnstocks.com/docs/tai-lieu/huong-dan-nhanh         ║
║                                                              ║
║   Cảm ơn bạn đã luôn đồng hành cùng Vnstock! ❤️              ║
║                                                              ║
╚══════════════════════════════════════════════════════════════╝
"""

html_message = """
<div style="background-color: #f0f8ff; border: 2px solid #4682b4; border-radius: 10px; padding: 20px; font-family: Arial, sans-serif;">
    <h2 style="color: #4682b4;">🚀 Xin chào! Có tin vui cho bạn đây! 🎉</h2>
    <p>Bạn đang dùng phiên bản phần mềm <code>vnstock3</code>.</p>
    <p>Hiện tại Vnstock đã hợp nhất tên gọi thư viện thành <code>vnstock</code>. Hay quá phải không nào? 😃</p>
    <h3 style="color: #20b2aa;">Hãy cập nhật ngay để trải nghiệm phiên bản mới nhất:</h3>
    <pre style="background-color: #f0f8ff; border: 1px solid #4682b4; padding: 10px; border-radius: 5px;"><code>pip install -U vnstock</code></pre>
    <p>Muốn biết thêm chi tiết? Ghé thăm: <a href="https://vnstocks.com/docs/tai-lieu/huong-dan-nhanh" style="color: #4682b4;">https://vnstocks.com/docs/tai-lieu/huong-dan-nhanh</a></p>
    <p style="color: #ff6347;">Cảm ơn bạn đã luôn đồng hành cùng Vnstock! ❤️</p>
</div>
"""

if is_notebook():
    from IPython.display import display, HTML
    display(HTML(html_message))
else:
    print(update_message)