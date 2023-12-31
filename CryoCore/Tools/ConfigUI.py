#!/usr/bin/env python3

import sys
import curses
from CryoCore import API
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

def check_confirm(window, message, yes_response, no_response):
    window.hline(0, 0, " ", window.getmaxyx()[1], curses.color_pair(4))
    window.addstr(message, curses.color_pair(4))
    window.refresh()
    c = window.getch()
    try:
        asc = chr(c)
        window.hline(0, 0, " ", window.getmaxyx()[1], curses.color_pair(4))
        if asc.lower() == "y" or asc.lower() == "yes":
            window.hline(0, 0, " ", window.getmaxyx()[1], curses.color_pair(6))
            window.addstr(0, 0, yes_response, curses.color_pair(6))
            window.refresh()
            return True
        else:
            window.addstr(0, 0, no_response, curses.color_pair(4))
            window.refresh()
            return False
        window.refresh()
    except:
        window.addstr(0, 0, f"{no_response} (exception)", curses.color_pair(4))
    return False


class Category:
    def __init__(s, name, lookupKey, cfg, level=0, parent=None):
        s.cfg = cfg
        s.name = name
        s.lookupKey = lookupKey
        s.categories = []
        s.level = level
        s.expand = len(name) == 0
        s.isValue = len(cfg.keys(lookupKey)) == 0
        s.filter = False
        s.parent = parent
        if s.isValue:
            s.setValue(cfg[lookupKey], None, False)
        for key in cfg.keys(lookupKey):
            if len(lookupKey) > 0:
                category = Category(key, lookupKey + "." + key, cfg, level + 1, s)
            else:
                category = Category(key, key, cfg, level + 1, s)
            s.categories.append(category)
    
    def remove(self):
        if len(self.lookupKey) > 0:
            self.cfg.remove(self.lookupKey)
            if self.parent is not None:
                self.parent.categories.remove(self)
    
    def update(self, lookupKey):
        if self.lookupKey == lookupKey:
            #log.debug(f"Found config param for key {lookupKey}")
            self.setValue(self.cfg[self.lookupKey], None, False)
            return True
        for c in self.categories:
            if c.update(lookupKey):
                return True
        return False
    
    def build_parameter_list(self, parameter_list):
        if self.isValue:
            parameter_list.append(self.lookupKey)
        for c in self.categories:
            c.build_parameter_list(parameter_list)
        return parameter_list
    
    def getDataType(s):
        native = s.value
        try:
            if "." in s.value:
                native = float(s.value)
            elif s.value.lower() == "true" or s.value.lower() == "false":
                native = eval(s.value)
            else:
                native = int(s.value)
        except:
            pass
        if native.__class__ == float:
            return "double"
        elif native.__class__ == int:
            return "integer"
        elif native.__class__ == bool:
            return "boolean"
        return "string"

    def setValue(s, newValue, window=None, commit=True):
        s.value = toStr(newValue)

        if s.value.__class__ == str:
            s.value = s.value.replace("\t", " ").replace("\n", " ")
        s.truncValue = s.value.encode(code)  # str(s.value)
        if len(s.truncValue) > truncateLength:
            s.truncValue = s.truncValue[0:truncateLength - 4] + "..".encode(code)
        if commit:
            try:
                s.cfg.set(s.lookupKey, s.value, None)
            except:
                if window is not None:
                    if check_confirm(window, "Error, datatype mismatch! Override? (Y/N): ", "Changed", "Not changed"):
                        s.cfg.set(s.lookupKey, s.value, datatype=s.getDataType())
                    s.setValue(s.cfg[s.lookupKey], None, False)

    def setRecursiveExpand(s, flag):
        s.expand = flag
        for i in range(0, len(s.categories)):
            s.categories[i].setRecursiveExpand(flag)

    def checkFilter(s, filterText):
        s.expand = False
        if filterText in s.lookupKey.lower() or (s.isValue and filterText in toStr(s.value).lower()):
            s.filter = True
            p = s.parent
            while p is not None:
                p.filter = True
                p = p.parent
        else:
            s.filter = False
        for i in range(0, len(s.categories)):
            s.categories[i].checkFilter(filterText)

    def toggleSetting(s, window):
        if s.isValue:
            if toStr(s.value) == "False":
                s.setValue(True, window)
            elif toStr(s.value) == "True":
                s.setValue(False, window)
            elif toStr(s.value) == "on":
                s.setValue("off", window)
            elif toStr(s.value) == "off":
                s.setValue("on", window)

    def clearFilter(s):
        s.expand = len(s.name) == 0
        s.filter = False
        for i in range(0, len(s.categories)):
            s.categories[i].clearFilter()

    def getVisible(s, list, enableFilter=False):
        if s.filter and enableFilter:
            list.append(s)
        elif not enableFilter:
            list.append(s)
        if s.expand or (s.filter and enableFilter):
            for i in range(0, len(s.categories)):
                s.categories[i].getVisible(list, enableFilter)
        return list

    def renderToScreen(s, screen, line, width, selected=False):
        try:
            marker = "> "
            if s.expand or s.filter:
                marker = "v "

            displayName = s.lookupKey if fullKeys else s.name
            x = s.level * 3
            if selected:
                color = curses.color_pair(3)
            elif s.isValue:
                color = curses.color_pair(0)
            else:
                color = curses.color_pair(0) | curses.A_BOLD
            if s.isValue:
                screen.hline(line, 0, " ", width, color)
                screen.addstr(line, x, displayName, color)
                if width - truncateLength - 2 > 0:
                    screen.addstr(line, width - truncateLength - 2, "| ".encode(code) + s.truncValue, color)
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
    def __init__(s, window, color, forceLower=False):
        s.set_window(window)
        s.initialValue = ""
        s.value = ""
        s.editing = False
        s.color = color
        s.cursorColor = curses.color_pair(2)
        s.cursorPos = 0
        s.forceLowerCase = forceLower
        s.saveOnEnd = False
    
    def set_window(self, window):
        self.window = window
        self.height, self.width = self.window.getmaxyx()
    
    def beginEditing(s, newInitialValue=None):
        if newInitialValue is not None:
            s.initialValue = newInitialValue
        s.value = s.initialValue
        s.editing = True
        s.cursorPos = len(s.value)
        s.saveOnEnd = False
        s.render()

    def consumeKey(s, c, asc, isAlt=False, doRender=True):
        if not s.editing:
            return False
        else:
            if c == curses.KEY_BACKSPACE or c == 8 or c == 13:
                if s.cursorPos > 0:
                    s.value = s.value[0:s.cursorPos - 1] + s.value[s.cursorPos:len(s.value)]
                    s.cursorPos -= 1
            elif c == curses.KEY_LEFT or (isAlt and curses.keyname(c) == 'b'):
                if s.cursorPos > 0:
                    if isAlt:
                        idx = s.value.rfind(" ", 0, s.cursorPos - 1)
                        s.cursorPos = idx if idx >= 0 else 0
                    else:
                        s.cursorPos -= 1
            elif c == curses.KEY_RIGHT or (isAlt and curses.keyname(c) == 'f'):
                if s.cursorPos < len(s.value):
                    if isAlt:
                        idx = s.value.find(" ", s.cursorPos + 1)
                        s.cursorPos = idx if idx >= 0 else len(s.value)
                    else:
                        s.cursorPos += 1
            elif c == curses.KEY_UP or c == curses.KEY_DOWN:
                cursX = s.cursorPos % s.width
                cursY = s.cursorPos / s.width
                if c == curses.KEY_UP:
                    cursY -= 1
                elif c == curses.KEY_DOWN:
                    cursY += 1
                s.cursorPos = max(0, min((s.width * cursY) + cursX, len(s.value)))
            elif c == 27:  # alt or escape
                newC = readEscape(s.window)
                if newC == -1:
                    s.value = s.initialValue
                    s.editing = False
                    s.saveOnEnd = False
                else:
                    newAsc = -1
                    try:
                        newAsc = chr(c)
                    except:
                        pass
                    s.consumeKey(newC, newAsc, True, False)
            elif asc == "\n":
                s.editing = False
                s.saveOnEnd = True
            elif asc != -1:
                if s.forceLowerCase:
                    asc = asc.lower()
                s.value = s.value[0:s.cursorPos] + asc + s.value[s.cursorPos:len(s.value)]
                s.cursorPos += 1
            if doRender:
                s.render()
            return True
        return False

    def render(s):
        color = s.color if s.editing else curses.color_pair(0)
        for y in range(0, s.height):
            s.window.hline(y, 0, " ", s.width, color)
        try:
            for y in range(0, s.height):
                x = y * s.width
                line = s.value[x:x + s.width]
                s.window.addstr(y, 0, line.encode(code), color)
            cursX = int(s.cursorPos % s.width)
            cursY = int(s.cursorPos / s.width)
            if s.cursorPos < len(s.value):
                s.window.addstr(cursY, cursX, s.value[s.cursorPos].encode(code), s.cursorColor)
            else:
                s.window.addstr(cursY, cursX, " ", s.cursorColor)
        except:
            if log: log.exception("Error in render")
            # Hopefully we'll never have settings that require more than 8 lines of space to render.
        s.window.refresh()

helpText = """The editor works as follows:
   Up/down arrow keys: Move selection
Left/right arrow keys: Expand or collapse a group of settings
                       Hold down the ALT key while pressing
                       left/right arrow to expand or collapse
                       an entire subgroup.
                    T: Toggle a setting between True/False or
                       on/off
                    /: Search for names and settings with the
                       given text. Press enter when you are
                       done, or ESC to clear the filter.
                    Q: Exit the application
                    ?: Show/hide this help text.
         Enter/return: Edit a setting. Does nothing if the
                       selection is not a group instead of a
                       setting.

When editing a setting, press Enter to save it, or ESC to exit
the editor without saving. Use the arrow keys to navigate the
editor, and alt-left/right to move the cursor word-by-word.

Press ? to exit help.
""".replace("\t", "    ")


class ConsoleUI:
    def __init__(self, cfg):
        self.cfg = cfg
        self.categories = Category("", "", cfg)
        parameter_list = self.categories.build_parameter_list([])
        self.cfg.add_callback(parameter_list, self.config_updated)
        self.help = helpText.split("\n")
        self.helpMode = False

    def config_updated(self, param):
        #log.info(f"{param.name} {param.id} {param.path} was updated to {param.get_value()}")
        if not self.categories.update(f"{param.path}.{param.name}"):
            log.warning(f"Failed to find config param for {param.path}.{param.name} - restart ccconfigui to discover new config params")
        
    def updateFilter(s):
        if len(s.filterEditor.value) > 0:
            s.selectedCategory = 0
            s.categories.checkFilter(s.filterEditor.value)
            s.visibleCategories = s.categories.getVisible([], True)
        else:
            s.selectedCategory = 0
            s.categories.clearFilter()
            s.visibleCategories = s.categories.getVisible([], False)

    def handleInput(s, c, asc, isAlt=False):
        if asc == '?':
            s.helpMode = not s.helpMode
            return
        if c == curses.KEY_UP:
            s.selectedCategory = s.selectedCategory if s.selectedCategory == 0 else s.selectedCategory - 1
        elif c == curses.KEY_DOWN:
            s.selectedCategory = s.selectedCategory if s.selectedCategory == len(s.visibleCategories) - 1 else s.selectedCategory + 1
        elif c == curses.KEY_RIGHT:
            s.visibleCategories[s.selectedCategory].expand = True
            s.visibleCategories = s.categories.getVisible([], len(s.filterEditor.value) > 0)
        elif c == curses.KEY_BACKSPACE or c == 8 or c == 13:
            if check_confirm(s.settingWindow, f"Delete config tree {s.visibleCategories[s.selectedCategory].lookupKey}? ", "Deleted", "Not deleted"):
                s.visibleCategories[s.selectedCategory].remove()
                s.visibleCategories = s.categories.getVisible([], len(s.filterEditor.value) > 0)
                if s.selectedCategory > len(s.visibleCategories):
                    s.selectedCategory = len(s.visibleCategories)-1
        elif isAlt and (curses.keyname(c) == 'f' or curses.keyname(c) == 'b'):  # right or left arrow
            s.visibleCategories[s.selectedCategory].setRecursiveExpand(curses.keyname(c) == 'f')
            s.visibleCategories = s.categories.getVisible([], len(s.filterEditor.value) > 0)
        elif c == 27:  # escape or alt
            newC = readEscape(s.screen)
            try:
                newAsc = chr(newC)
            except:
                pass
            if newC == -1:
                # escape!
                pass
            else:
                s.handleInput(newC, newAsc, True)
        elif c == curses.KEY_LEFT:
            # If the selection is already unexpanded, toggle parent and select it
            if not s.visibleCategories[s.selectedCategory].expand and s.visibleCategories[s.selectedCategory].parent:
                s.visibleCategories[s.selectedCategory].parent.expand = False
                for i in range(0, len(s.visibleCategories)):
                    if s.visibleCategories[i] == s.visibleCategories[s.selectedCategory].parent:
                        s.selectedCategory = i
                        break
            else:
                s.visibleCategories[s.selectedCategory].expand = False
            s.visibleCategories = s.categories.getVisible([], len(s.filterEditor.value) > 0)
        elif asc == '\n':
            if s.visibleCategories[s.selectedCategory].isValue:
                s.settingEditor.beginEditing(toStr(s.visibleCategories[s.selectedCategory].value))
        elif asc == '/':
            s.inputColor = curses.color_pair(5)
            s.filterEditor.beginEditing()
            s.updateFilter()
        elif asc == 't':
            s.visibleCategories[s.selectedCategory].toggleSetting(s.settingWindow)
        elif asc == "q":
            API.api_stop_event.set()

    def getInput(s):
        c = s.screen.getch()
        if c == -1:
            return
        if c == curses.KEY_RESIZE:
            s.resize_screen()
            return
        asc = -1
        try:
            asc = chr(c)
        except:
            pass
        if s.filterEditor.consumeKey(c, asc):
            s.updateFilter()
            return
        if s.settingEditor.consumeKey(c, asc):
            if not s.settingEditor.editing:
                if s.settingEditor.saveOnEnd:
                    s.visibleCategories[s.selectedCategory].setValue(s.settingEditor.value, s.settingWindow)
            return
        s.handleInput(c, asc)

    def renderCategories(s):
        while s.categoryHeight + s.categoryScroll <= s.selectedCategory:
            s.categoryScroll += 1
        while s.categoryScroll >= s.selectedCategory:
            s.categoryScroll -= 1
        if s.categoryScroll >= len(s.visibleCategories) - s.categoryHeight:
            s.categoryScroll = len(s.visibleCategories) - s.categoryHeight
        if s.categoryScroll < 0:
            s.categoryScroll = 0
        for i in range(0, s.categoryHeight):
            if i + s.categoryScroll >= len(s.visibleCategories):
                s.categoryWindow.hline(i, 0, " ", s.width)
                continue
            s.visibleCategories[i + s.categoryScroll].renderToScreen(s.categoryWindow, i, s.width, i + s.categoryScroll == s.selectedCategory)

        if len(s.visibleCategories) > 0:
            scrollBarLength = float(s.categoryHeight) / float(len(s.visibleCategories))
            scrollBarStart = float(s.categoryScroll) / float(len(s.visibleCategories))
            for i in range(0, s.categoryHeight):
                pos = float(i) / float(s.categoryHeight)
                if pos >= scrollBarStart and pos <= (scrollBarStart + scrollBarLength):
                    s.categoryWindow.addstr(i, 0, "# ")
                else:
                    s.categoryWindow.addstr(i, 0, "  ")
        s.categoryWindow.refresh()

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
    def run(s, screen):
        global truncateLength
        s.screen = screen
        s.screen.timeout(200)
        # Set up some colors
        curses.init_pair(1, curses.COLOR_WHITE, curses.COLOR_BLUE)
        curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_WHITE)
        curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_CYAN)
        curses.init_pair(4, curses.COLOR_WHITE, curses.COLOR_RED)
        curses.init_pair(5, curses.COLOR_BLACK, curses.COLOR_YELLOW)
        curses.init_pair(6, curses.COLOR_WHITE, curses.COLOR_GREEN)
        try:
            curses.curs_set(0)
        except:
            pass
        s.promptStack = []
        s.borderColor = curses.color_pair(4)
        s.inputColor = curses.color_pair(5)
        s.infoHeight = 4
        s.footerHeight = 2
        s.headerHeight = 1
        s.resize_screen()

        s.pushPrompt("Arrow keys: Move/expand selection | / : Filter | T: Toggle setting | Enter: Modify setting | Q: Exit | ? : Help", False)
        s.screen.hline(s.height - 1, 0, " ", s.width, s.inputColor)
        s.screen.hline(s.height - 1, 0, " ", s.width)
        #s.screen.addstr(0, 0, "Width: %d Height: %d" % (s.width, s.height))
        s.screen.refresh()
        curses.doupdate()
        s.selectedCategory = 0
        s.categoryScroll = 0
        s.visibleCategories = s.categories.getVisible([])
        while not API.api_stop_event.is_set():
            s.refresh()
            s.getInput()
    
    def refresh(self):
        if self.helpMode:
            for y in range(0, self.categoryWindow.getmaxyx()[0]):
                self.categoryWindow.hline(y, 0, " ", self.width)
                if y < len(self.help):
                    self.categoryWindow.addstr(y, 0, self.help[y])
            self.categoryWindow.refresh()
        else:
            self.renderCategories()
        if self.settingEditor.editing:
            self.settingEditor.render()
        elif self.filterEditor.editing:
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
        if hasattr(self, "settingEditor"):
            self.settingEditor.set_window(self.settingWindow)
            self.filterEditor.set_window(self.filterWindow)
        else:
            self.settingEditor = Editor(self.settingWindow, curses.color_pair(5))
            self.filterEditor = Editor(self.filterWindow, curses.color_pair(5), True)
        self.screen.clear()
        self.screen.hline(0, 0, "*", self.width, self.borderColor)
        self.centerText(self.screen, 0, "CryoCore Config Tool", self.borderColor, 3)
    
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

    def pushPrompt(s, text, centered=False):
        s.promptStack.append([text, centered])
        s.setPrompt(text, centered)

    def setPrompt(s, text, centered):
        try:
            s.screen.hline(s.height - 2, 0, " ", s.width, s.borderColor)
            if centered:
                s.centerText(s.screen, s.height - 2, text, s.borderColor)
            else:
                s.screen.addstr(s.height - 2, 0, text[0:s.width], s.borderColor)
        except:
            # Exceptions here are typically caused during resize. We don't want
            # to spew the log with them.
            pass

    def popPrompt(s):
        s.promptStack = s.promptStack[0:-1]
        s.setPrompt(s.promptStack[-1][0], s.promptStack[-1][1])
    
    def refreshPrompt(self):
        self.setPrompt(self.promptStack[-1][0], self.promptStack[-1][1])

if __name__ == "__main__":
    parser = ArgumentParser(description="Tree view of CryoCore configuration")
    parser.add_argument("--fullkeys", action="store_true", default=False, help="Use full keys")
    parser.add_argument("--stacks", action="store_true", default=False, help="Print all thread stacks after shutdown")
    parser.add_argument("-v", "--version", dest="version", default=None, help="Version to operate on")

    options = parser.parse_args()
    if options.fullkeys:
        fullkeys = True
    try:
        cfg = None
        log = API.get_log("CryoCore.Tools.ConfigUI")
        #status = API.get_status("CryoCore.Tools.ConfigUI")
        if options.version:
            try:
                cfg.set_version(options.version)
                cfg = API.get_config(version=options.version)
            except Config.NoSuchVersionException:
                raise SystemExit("version <%s> does not exist in list: <%s>. Aborting." %
                                 (options.version, ",".join(map(str, cfg.list_versions()))))
        else:
            cfg = API.get_config()
        ui = ConsoleUI(cfg)
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
