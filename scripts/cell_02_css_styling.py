# cell_02_css_styling.py — Shared CSS for all report cells

from IPython.display import display, HTML

display(HTML("""
<style>
    .peec-header {
        font-size: 20px; font-weight: 700; color: #1a1a2e;
        margin: 10px 0 5px 0; font-family: 'Helvetica Neue', sans-serif;
    }
    .peec-sub {
        font-size: 13px; color: #666; margin-bottom: 15px;
        font-family: 'Helvetica Neue', sans-serif;
    }
    .peec-stat {
        display: inline-block; background: #f0f4ff; border-radius: 8px;
        padding: 8px 16px; margin: 4px 6px 4px 0; font-size: 13px;
        font-family: 'Helvetica Neue', sans-serif;
    }
    .peec-stat b { color: #1a1a2e; }
    .peec-section {
        font-size: 12px; font-weight: 600; color: #999;
        text-transform: uppercase; letter-spacing: 0.5px; margin: 8px 0 4px 0;
        font-family: 'Helvetica Neue', sans-serif;
    }
    .peec-scroll {
        max-height: 600px; overflow-y: auto; overflow-x: auto;
        border: 1px solid #e0e0e0; border-radius: 6px; margin-top: 8px;
    }
    .peec-scroll table { font-size: 12px; border-collapse: collapse; width: 100%; }
    .peec-scroll th {
        position: sticky; top: 0; background: #f8f9fa;
        border-bottom: 2px solid #dee2e6; padding: 8px 10px; text-align: left;
    }
    .peec-scroll td { padding: 6px 10px; border-bottom: 1px solid #eee; }
    .peec-scroll tr:hover td { background: #f0f4ff; }
</style>
"""))
print("\u2705 Styles loaded.")
