#!/usr/bin/env python3

import sys
import curses
import time
from CryoCore import API
from CryoCore.Core.Status.StatusListener import StatusListener
from CryoCore.Tools.TailStatus import TailStatus
API.queue_timeout = 0.1
API.shutdown_grace_period = 0.0

import CryoCore.Core.Config as Config
import locale
import traceback
from argparse import ArgumentParser

locale.setlocale(locale.LC_ALL, '')
code = locale.getpreferredencoding()

ui = None
truncateLength = 80
fullKeys = False
log = None
status = None

def startUI(screen):
    ui.run(screen)


def toStr(val):
    if (sys.version_info.major == 2):
        ret = unicode(val)
    else:
        if val.__class__ == "bytes":
            ret = str(val, "utf-8")
        else:
            ret = str(val)
    return ret

class StatusValue:
    def __init__(self, path, value, is_leaf, level=0, parent=None, timestamp=None):
        self.path = path
        self.name = path.split(".")[-1]
        self.level = level
        self.is_leaf = is_leaf
        if self.is_leaf:
            self.value = value
            self.timestamp = timestamp or time.time()
        else:
            self.value = None
            self.timestamp = None
        self.expand = len(self.name) == 0
        self.filter = False
        self.parent = parent
        self.children = []
        self.child_map = {}
        self.flash = 0
    
    def measure_label(self, current=0):
        current = max((self.level * 3) + len(self.name), current)
        for child in self.children:
            current = max(current, child.measure_label(current))
        return current
    
    def setRecursiveExpand(self, flag):
        self.expand = flag
        for child in self.children:
            child.setRecursiveExpand(flag)

    def checkFilter(self, filterText):
        self.expand = False
        if filterText in self.path.lower():
            self.filter = True
            p = self.parent
            while p is not None:
                p.filter = True
                p = p.parent
        else:
            self.filter = False
        for child in self.children:
            child.checkFilter(filterText)

    def clearFilter(self):
        #self.expand = len(self.name) == 0 or self.
        self.filter = False
        for child in self.children:
            child.clearFilter()

    def getVisible(self, result, enableFilter, filter_recent, recent_seconds):
        if self.is_leaf and filter_recent:
            if self.timestamp is None:
                print("Error, leaf value doesn't have a timestamp")
            else:
                if time.time()-self.timestamp > recent_seconds:
                    return result
        visible_children = []
        if self.expand or (self.filter and enableFilter):
            for child in self.children:
                child.getVisible(visible_children, enableFilter, filter_recent, recent_seconds)
        if len(visible_children) > 0 or (not enableFilter and not filter_recent):
            result.append(self)
            result += visible_children
        return result
    
    def update_value(self, value, timestamp):
        self.is_leaf = True
        if value != self.value:
            self.flash = 2
        self.value = value
        self.timestamp = timestamp or time.time()
    
    def add_or_update(self, path, value, timestamp=None):
        names = path.split(".")
        node_path = ""
        parent = self
        added = False
        for index in range(0, len(names)):
            key = ".".join(names[0:index+1])
            if key in parent.child_map:
                parent = parent.child_map[key]
            else:
                child = StatusValue(key, None, False, parent.level+1, parent, timestamp)
                parent.children.append(child)
                parent.child_map[key] = child
                parent = child
                added = True
        parent.update_value(value, timestamp)
        return added
    
    def renderToScreen(self, screen, line, width, selected=False):
        try:
            marker = "> "
            if self.expand or self.filter:
                marker = "v "

            displayName = self.name
            x = self.level * 3
            if selected:
                color = curses.color_pair(3)
            elif self.is_leaf:
                color = curses.color_pair(0)
            else:
                color = curses.color_pair(2)
            if self.is_leaf:
                if self.flash > 0:
                    self.flash -= 1
                    color = curses.color_pair(4)
                screen.hline(line, 0, " ", width, color)
                screen.addstr(line, x, displayName, color)
                if width - truncateLength - 2 > 0:
                    screen.addstr(line, width - truncateLength - 2, "| ".encode(code) + f"{self.value}".encode(code), color)
            else:
                displayName = marker + displayName
                screen.hline(line, 0, " ", width, color)
                screen.addstr(line, x, displayName, color)
                screen.hline(line, x + len(displayName) + 1, "-", width - (x + len(displayName)) - truncateLength, color)
                if width - truncateLength - 2 > 0:
                    screen.hline(line, width - truncateLength, "=", truncateLength, color)
                    screen.addstr(line, width - truncateLength - 2, "+=", color)
        except:
            if log: log.exception("Error rendering to screen")


def readEscape(window):
    window.nodelay(True)
    c = window.getch()
    window.nodelay(False)
    return c


class Editor:
    def __init__(self, window, color, forceLower=False):
        self.set_window(window)
        self.initialValue = ""
        self.value = ""
        self.editing = False
        self.color = color
        self.cursorColor = curses.color_pair(2)
        self.cursorPos = 0
        self.forceLowerCase = forceLower
        self.saveOnEnd = False
    
    def set_window(self, window):
        self.window = window
        self.height, self.width = self.window.getmaxyx()
    
    def beginEditing(self, newInitialValue=None):
        if newInitialValue is not None:
            self.initialValue = newInitialValue
        self.value = self.initialValue
        self.editing = True
        self.cursorPos = len(self.value)
        self.saveOnEnd = False
        self.render()

    def consumeKey(self, c, asc, isAlt=False, doRender=True):
        if not self.editing:
            return False
        else:
            if c == curses.KEY_BACKSPACE or c == 8 or c == 13:
                if self.cursorPos > 0:
                    self.value = self.value[0:self.cursorPos - 1] + self.value[self.cursorPos:len(self.value)]
                    self.cursorPos -= 1
            elif c == curses.KEY_LEFT or (isAlt and curses.keyname(c) == 'b'):
                if self.cursorPos > 0:
                    if isAlt:
                        idx = self.value.rfind(" ", 0, self.cursorPos - 1)
                        self.cursorPos = idx if idx >= 0 else 0
                    else:
                        self.cursorPos -= 1
            elif c == curses.KEY_RIGHT or (isAlt and curses.keyname(c) == 'f'):
                if self.cursorPos < len(self.value):
                    if isAlt:
                        idx = self.value.find(" ", self.cursorPos + 1)
                        self.cursorPos = idx if idx >= 0 else len(self.value)
                    else:
                        self.cursorPos += 1
            elif c == curses.KEY_UP or c == curses.KEY_DOWN:
                cursX = self.cursorPos % self.width
                cursY = self.cursorPos / self.width
                if c == curses.KEY_UP:
                    cursY -= 1
                elif c == curses.KEY_DOWN:
                    cursY += 1
                self.cursorPos = max(0, min((self.width * cursY) + cursX, len(self.value)))
            elif c == 27:  # alt or escape
                newC = readEscape(self.window)
                if newC == -1:
                    self.value = self.initialValue
                    self.editing = False
                    self.saveOnEnd = False
                else:
                    newAsc = -1
                    try:
                        newAsc = chr(c)
                    except:
                        pass
                    self.consumeKey(newC, newAsc, True, False)
            elif asc == "\n":
                self.editing = False
                self.saveOnEnd = True
            elif asc != -1:
                if self.forceLowerCase:
                    asc = asc.lower()
                self.value = self.value[0:self.cursorPos] + asc + self.value[self.cursorPos:len(self.value)]
                self.cursorPos += 1
            if doRender:
                self.render()
            return True
        return False

    def render(self):
        color = self.color if self.editing else curses.color_pair(0)
        for y in range(0, self.height):
            self.window.hline(y, 0, " ", self.width, color)
        try:
            for y in range(0, self.height):
                x = y * self.width
                line = self.value[x:x + self.width]
                self.window.addstr(y, 0, line.encode(code), color)
            cursX = int(self.cursorPos % self.width)
            cursY = int(self.cursorPos / self.width)
            if self.cursorPos < len(self.value):
                self.window.addstr(cursY, cursX, self.value[self.cursorPos].encode(code), self.cursorColor)
            else:
                self.window.addstr(cursY, cursX, " ", self.cursorColor)
        except:
            if log: log.exception("Error in render")
            # Hopefully we'll never have settings that require more than 8 lines of space to render.
        self.window.refresh()

helpText = """The editor works as follows:
   Up/down arrow keys: Move selection
Left/right arrow keys: Expand or collapse a group of settings
                       Hold down the ALT key while pressing
                       left/right arrow to expand or collapse
                       an entire subgroup.
                    /: Search for names and settings with the
                       given text. Press enter when you are
                       done, or ESC to clear the filter.
                    *: Expand or collapse all status variables
                    $: Toggle filtering of not recently updated
                       variables.
                    Q: Exit the application
                    ?: Show/hide this help text.

Press ? to exit help.
""".replace("\t", "    ")


class ConsoleUI:
    def __init__(self, options):
        self.root = StatusValue("", None, False)
        self.help = helpText.split("\n")
        self.helpMode = False
        self.import_initial_status(options.allow_none)
        self.listener = StatusListener(monitor_all=True)
        self.listener._bus_sleep = 0.2
        self.expand_all = True
        self.filter_recent = True
        self.recent_seconds = options.recent_seconds
        self.root.setRecursiveExpand(self.expand_all)
    
    def import_initial_status(self, allow_none):
        print("Importing existing status.. stand by.")
        ts = TailStatus("Tools.StatusUI", None)
        ts.create_pc_index()
        channels = ts.get_channels()
        for channel in channels:
            params = ts.get_params(channel)
            #print(f"{channel} : {params}")
            for param in params:
                last_value, timestamp = ts.get_last_value(channel, param)
                if last_value is not None or allow_none:
                    self.root.add_or_update(f"{channel}.{param}", last_value, timestamp)
        print("Ready!")
    
    def updateFilter(self):
        if len(self.filterEditor.value) > 0:
            self.selected = 0
            self.root.checkFilter(self.filterEditor.value)
            self.visible = self.root.getVisible([], True, self.filter_recent, self.recent_seconds)
        else:
            self.selected = 0
            self.root.clearFilter()
            self.visible = self.root.getVisible([], False, self.filter_recent, self.recent_seconds)

    def handleInput(self, c, asc, isAlt=False):
        if asc == '?':
            self.helpMode = not self.helpMode
            return
        if c == curses.KEY_UP:
            self.selected = self.selected if self.selected == 0 else self.selected - 1
        elif c == curses.KEY_DOWN:
            self.selected = self.selected if self.selected == len(self.visible) - 1 else self.selected + 1
        elif c == curses.KEY_RIGHT:
            self.visible[self.selected].expand = True
            self.visible = self.root.getVisible([], len(self.filterEditor.value) > 0, self.filter_recent, self.recent_seconds)
        elif isAlt and (curses.keyname(c) == 'f' or curses.keyname(c) == 'b'):  # right or left arrow
            self.visible[self.selected].setRecursiveExpand(curses.keyname(c) == 'f')
            self.visible = self.root.getVisible([], len(self.filterEditor.value) > 0, self.filter_recent, self.recent_seconds)
        elif c == 27:  # escape or alt
            newC = readEscape(self.screen)
            try:
                newAsc = chr(newC)
            except:
                pass
            if newC == -1:
                # escape!
                pass
            else:
                self.handleInput(newC, newAsc, True)
        elif c == curses.KEY_LEFT:
            # If the selection is already unexpanded, toggle parent and select it
            if not self.visible[self.selected].expand and self.visible[self.selected].parent:
                self.visible[self.selected].parent.expand = False
                for i in range(0, len(self.visible)):
                    if self.visible[i] == self.visible[self.selected].parent:
                        self.selected = i
                        break
            else:
                self.visible[self.selected].expand = False
            self.visible = self.root.getVisible([], len(self.filterEditor.value) > 0, self.filter_recent, self.recent_seconds)
        elif asc == '*':
            self.expand_all = not self.expand_all
            self.root.setRecursiveExpand(self.expand_all)
        elif asc == '$':
            self.filter_recent = not self.filter_recent
        elif asc == '/':
            self.inputColor = curses.color_pair(5)
            self.filterEditor.beginEditing()
            self.updateFilter()
        elif asc == "q":
            API.api_stop_event.set()

    def getInput(self):
        c = self.screen.getch()
        if c == -1:
            return
        if c == curses.KEY_RESIZE:
            self.resize_screen()
            return
        asc = -1
        try:
            asc = chr(c)
        except:
            pass
        if self.filterEditor.consumeKey(c, asc):
            self.updateFilter()
            return
        self.handleInput(c, asc)

    def renderTree(self):
        while self.categoryHeight + self.categoryScroll <= self.selected:
            self.categoryScroll += 1
        while self.categoryScroll >= self.selected:
            self.categoryScroll -= 1
        if self.categoryScroll >= len(self.visible) - self.categoryHeight:
            self.categoryScroll = len(self.visible) - self.categoryHeight
        if self.categoryScroll < 0:
            self.categoryScroll = 0
        for i in range(0, self.categoryHeight):
            if i + self.categoryScroll >= len(self.visible):
                self.categoryWindow.hline(i, 0, " ", self.width)
                continue
            self.visible[i + self.categoryScroll].renderToScreen(self.categoryWindow, i, self.width, i + self.categoryScroll == self.selected)

        if len(self.visible) > 0:
            scrollBarLength = float(self.categoryHeight) / float(len(self.visible))
            scrollBarStart = float(self.categoryScroll) / float(len(self.visible))
            for i in range(0, self.categoryHeight):
                pos = float(i) / float(self.categoryHeight)
                if pos >= scrollBarStart and pos <= (scrollBarStart + scrollBarLength):
                    self.categoryWindow.addstr(i, 0, "# ")
                else:
                    self.categoryWindow.addstr(i, 0, "  ")
        self.categoryWindow.refresh()

    """
    Screen layout:
    0 [ Header ]
    1 [ Categories/variables
        .
        .
      ]
    H-10 [ settings editor ]
    Bottom-1 [ Quick usage ]
    Bottom [ Input ]
    """
    def run(self, screen):
        global truncateLength
        self.screen = screen
        self.screen.timeout(200)
        # Set up some colors
        curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLUE)
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_WHITE)
        curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_CYAN)
        curses.init_pair(4, curses.COLOR_WHITE, curses.COLOR_RED)
        curses.init_pair(5, curses.COLOR_BLACK, curses.COLOR_YELLOW)
        curses.init_pair(6, curses.COLOR_BLACK, curses.COLOR_GREEN)
        try:
            curses.curs_set(0)
        except:
            pass
        self.promptStack = []
        self.borderColor = curses.color_pair(6)
        self.inputColor = curses.color_pair(5)
        self.infoHeight = 4
        self.footerHeight = 2
        self.headerHeight = 1
        self.resize_screen()

        self.pushPrompt("Arrows: Move/expand | / : Filter | * : Expand/collapse all : $ : Toggle recent | Q: Exit | ? : Help", False)
        self.screen.hline(self.height - 1, 0, " ", self.width, self.inputColor)
        self.screen.hline(self.height - 1, 0, " ", self.width)
        self.screen.refresh()
        curses.doupdate()
        self.selected = 0
        self.categoryScroll = 0
        self.visible = self.root.getVisible([], False, self.filter_recent, self.recent_seconds)
        while not API.api_stop_event.is_set():
            self.refresh()
            self.getInput()
            status = self.listener.get_last_values()
            added = False
            for key, value in status.items():
                path = ".".join(key)
                added |= self.root.add_or_update(path, value["value"])
            if added and len(self.filterEditor.value) > 0:
                self.root.checkFilter(self.filterEditor.value)
            self.visible = self.root.getVisible([], len(self.filterEditor.value) > 0, self.filter_recent, self.recent_seconds)
            truncateLength = self.width - self.root.measure_label() - 10
    
    def refresh(self):
        if self.helpMode:
            for y in range(0, self.categoryWindow.getmaxyx()[0]):
                self.categoryWindow.hline(y, 0, " ", self.width)
                if y < len(self.help):
                    self.categoryWindow.addstr(y, 0, self.help[y])
            self.categoryWindow.refresh()
        else:
            self.renderTree()
        if self.filterEditor.editing:
            self.filterEditor.render()
        self.refreshPrompt()
        self.screen.refresh()
        try:
            self.screen.move(0, 0)
        except:
            if log: log.exception("Error moving")
        curses.doupdate()

    
    def resize_screen(self):
        self.height, self.width = self.screen.getmaxyx()
        truncateLength = int(self.width / 2)
        self.categoryHeight = self.height - self.infoHeight - self.footerHeight - self.headerHeight
        self.categoryWidth = self.width
        self.categoryWindow = curses.newwin(self.categoryHeight, self.categoryWidth, self.headerHeight, 0)
        self.settingWindow = curses.newwin(self.infoHeight, self.width, 1 + self.categoryHeight, 0)
        self.filterWindow = curses.newwin(1, self.width, self.height - 1, 0)
        if hasattr(self, "filterEditor"):
            self.filterEditor.set_window(self.filterWindow)
        else:
            self.filterEditor = Editor(self.filterWindow, curses.color_pair(5), True)
        self.screen.clear()
        self.screen.hline(0, 0, "*", self.width, self.borderColor)
        self.centerText(self.screen, 0, "CryoCore Status Monitor", self.borderColor, 3)
    
    def centerText(s, screen, line, text, color, pad=0):
        width = screen.getmaxyx()[1]
        start = int((width - (2 * pad + len(text))) / 2)
        if start < 0:
            start = 0
        pad = " " * pad
        text = "%s%s%s" % (pad, text, pad)
        if text.__class__ == "bytes":
            text = str(text, "utf-8")
        screen.addstr(line, start, text[0:width], color)

    def pushPrompt(self, text, centered=False):
        self.promptStack.append([text, centered])
        self.setPrompt(text, centered)

    def setPrompt(self, text, centered):
        try:
            self.screen.hline(self.height - 2, 0, " ", self.width, self.borderColor)
            if centered:
                self.centerText(self.screen, self.height - 2, text, self.borderColor)
            else:
                self.screen.addstr(self.height - 2, 0, text[0:self.width], self.borderColor)
        except:
            # Exceptions here are typically caused during resize. We don't want
            # to spew the log with them.
            pass

    def popPrompt(self):
        self.promptStack = self.promptStack[0:-1]
        self.setPrompt(self.promptStack[-1][0], self.promptStack[-1][1])
    
    def refreshPrompt(self):
        self.setPrompt(self.promptStack[-1][0], self.promptStack[-1][1])

if __name__ == "__main__":
    parser = ArgumentParser(description="Tree view of CryoCore configuration")
    parser.add_argument("--fullkeys", action="store_true", default=False, help="Use full keys")
    parser.add_argument("--stacks", action="store_true", default=False, help="Print all thread stacks after shutdown")
    parser.add_argument("--allow-none", action="store_true", default=False, help="Include status keys that don't have a value yet")
    parser.add_argument("--recent-seconds", action="store", default=3*60*60, type=float, help="Time delta since now for filtering not recently updated status items")
    options = parser.parse_args()
    if options.fullkeys:
        fullkeys = True
    try:
        log = API.get_log("CryoCore.Tools.StatusUI")
        #status = API.get_status("CryoCore.Tools.ConfigUI")
        ui = ConsoleUI(options)
        curses.wrapper(startUI)
    finally:
        #status.queue_callback(None)
        API.shutdown()
        if options.stacks:
            import threading, traceback, sys
            for thread in threading.enumerate():
                try:
                    print(thread)
                    traceback.print_stack(sys._current_frames()[thread.ident])
                    print("\n")
                except:
                    print("Thread already exited")
