"""Тёмная тема приложения (QSS)."""

DARK_QSS = """
* {
    font-family: "Segoe UI", "Inter", sans-serif;
    font-size: 13px;
    color: #e4e6eb;
}

QMainWindow, QWidget {
    background-color: #1a1d23;
}

/* ---------- Sidebar ---------- */
#Sidebar {
    background-color: #15171c;
    border-right: 1px solid #2a2e37;
}
#SidebarButton {
    text-align: left;
    padding: 12px 18px;
    border: none;
    border-radius: 8px;
    background: transparent;
    color: #9aa0ac;
    font-size: 14px;
}
#SidebarButton:hover {
    background-color: #22252d;
    color: #ffffff;
}
#SidebarButton:checked {
    background-color: #2d6cdf;
    color: #ffffff;
    font-weight: 600;
}
#SidebarLogo {
    color: #ffffff;
    font-size: 18px;
    font-weight: 700;
    padding: 18px;
}

/* ---------- Cards ---------- */
#StatCard {
    background-color: #22252d;
    border: 1px solid #2a2e37;
    border-radius: 12px;
}
#StatCardTitle { color: #9aa0ac; font-size: 12px; }
#StatCardValue { color: #ffffff; font-size: 28px; font-weight: 700; }

/* ---------- Buttons ---------- */
QPushButton {
    background-color: #2d6cdf;
    color: #ffffff;
    border: none;
    border-radius: 8px;
    padding: 9px 18px;
    font-weight: 600;
}
QPushButton:hover    { background-color: #3a78e8; }
QPushButton:disabled { background-color: #3a3f4b; color: #6b7280; }

QPushButton#DangerButton          { background-color: #d9534f; }
QPushButton#DangerButton:hover    { background-color: #e0625e; }
QPushButton#DangerButton:disabled { background-color: #3a3f4b; }

/* ---------- Inputs ---------- */
QLineEdit, QSpinBox, QComboBox, QPlainTextEdit {
    background-color: #15171c;
    border: 1px solid #2a2e37;
    border-radius: 6px;
    padding: 6px 8px;
    selection-background-color: #2d6cdf;
}
QLineEdit:focus, QSpinBox:focus, QComboBox:focus {
    border: 1px solid #2d6cdf;
}

/* ---------- Tables ---------- */
QTableView {
    background-color: #1a1d23;
    alternate-background-color: #1f2229;
    gridline-color: #2a2e37;
    border: 1px solid #2a2e37;
    border-radius: 8px;
    selection-background-color: #2d6cdf;
}
QHeaderView::section {
    background-color: #22252d;
    color: #9aa0ac;
    padding: 8px;
    border: none;
    border-bottom: 1px solid #2a2e37;
    font-weight: 600;
}
QTableView::item:hover { background-color: #262a33; }

/* ---------- Log view ---------- */
#LogView {
    background-color: #0e1014;
    border: 1px solid #2a2e37;
    border-radius: 8px;
    font-family: "Cascadia Code", "Consolas", monospace;
    font-size: 12px;
}

/* ---------- Progress ---------- */
QProgressBar {
    background-color: #15171c;
    border: 1px solid #2a2e37;
    border-radius: 6px;
    text-align: center;
    height: 18px;
}
QProgressBar::chunk {
    background-color: #2d6cdf;
    border-radius: 5px;
}

/* ---------- Scrollbars ---------- */
QScrollBar:vertical {
    background: #15171c; width: 10px; margin: 0;
}
QScrollBar::handle:vertical {
    background: #3a3f4b; border-radius: 5px; min-height: 30px;
}
QScrollBar::handle:vertical:hover { background: #4a505e; }
QScrollBar::add-line, QScrollBar::sub-line { height: 0; }

/* ---------- Status labels ---------- */
#StatusRunning { color: #4ade80; font-weight: 600; }
#StatusStopped { color: #9aa0ac; font-weight: 600; }
#StatusError   { color: #f87171; font-weight: 600; }
"""