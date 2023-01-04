pub enum NSKeys {
    A,
    B,
    C,
    D,
    E,
    F,
    G,
    H,
    I,
    J,
    K,
    L,
    M,
    N,
    O,
    P,
    Q,
    R,
    S,
    T,
    U,
    V,
    W,
    X,
    Y,
    Z,

    Num0,
    Num1,
    Num2,
    Num3,
    Num4,
    Num5,
    Num6,
    Num7,
    Num8,
    Num9,

    Apostrophe,
    BackApostrophe,
    Backslash,
    Backspace,
    CapsLock,
    Comma,
    Command,
    Control,
    Delete,
    Equals,
    Escape,
    FrontSlash,
    LeftBracket,
    Minus,
    Option,
    Period,
    Return,
    RightBracket,
    Semicolon,
    Shift,
    Tab,
    Space,

    Up,
    Left,
    Down,
    Right,
}

impl NSKeys {
    /// sourced from here:
    /// https://github.com/phracker/MacOSX-SDKs/blob/master/MacOSX10.6.sdk/System/Library/Frameworks/Carbon.framework/Versions/A/Frameworks/HIToolbox.framework/Versions/A/Headers/Events.h
    pub fn from(key_code: u16) -> Option<Self> {
        use NSKeys::*;
        let key = match key_code {
            0 => A,
            1 => S,
            2 => D,
            3 => F,
            4 => H,
            5 => G,
            6 => Z,
            7 => X,
            8 => C,
            9 => V,
            11 => B,
            12 => Q,
            13 => W,
            14 => E,
            15 => R,
            16 => Y,
            17 => T,
            18 => Num1,
            19 => Num2,
            20 => Num3,
            21 => Num4,
            22 => Num6,
            23 => Num5,
            24 => Equals,
            25 => Num9,
            26 => Num7,
            27 => Minus,
            28 => Num8,
            29 => Num0,
            30 => RightBracket,
            31 => O,
            32 => U,
            33 => LeftBracket,
            34 => I,
            35 => P,
            36 => Return,
            37 => L,
            38 => J,
            39 => Apostrophe,
            40 => K,
            41 => Semicolon,
            42 => FrontSlash,
            43 => Comma,
            44 => Backslash,
            45 => N,
            46 => M,
            47 => Period,
            48 => Tab,
            50 => BackApostrophe,
            51 => Backspace,
            53 => Escape,
            55 => Command,
            56 => Shift,
            57 => CapsLock,
            58 => Option,
            59 => Control,
            126 => Up,
            125 => Down,
            123 => Left,
            124 => Right,
            0x31 => Space,
            _ => return None,
        };

        Some(key)
    }

    pub fn valid_text(&self) -> bool {
        use NSKeys::*;
        match self {
            A | B | C | D | E | F | G | H | I | J | K | L | M | N | O | P | Q | R | S | T | U
            | V | W | X | Y | Z | Num0 | Num1 | Num2 | Num3 | Num4 | Num5 | Num6 | Num7 | Num8
            | Num9 | Apostrophe | BackApostrophe | Backslash | Comma | Equals | FrontSlash
            | LeftBracket | Minus | Period | RightBracket | Semicolon | Space => true,

            CapsLock | Command | Control | Delete | Escape | Option | Return | Shift | Tab | Up
            | Left | Down | Right | Backspace => false,
        }
    }

    pub fn egui_key(&self) -> Option<egui::Key> {
        use NSKeys::*;
        let key = match self {
            A => egui::Key::A,
            B => egui::Key::B,
            C => egui::Key::C,
            D => egui::Key::D,
            E => egui::Key::E,
            F => egui::Key::F,
            G => egui::Key::G,
            H => egui::Key::H,
            I => egui::Key::I,
            J => egui::Key::J,
            K => egui::Key::K,
            L => egui::Key::L,
            M => egui::Key::M,
            N => egui::Key::N,
            O => egui::Key::O,
            P => egui::Key::P,
            Q => egui::Key::Q,
            R => egui::Key::R,
            S => egui::Key::S,
            T => egui::Key::T,
            U => egui::Key::U,
            V => egui::Key::V,
            W => egui::Key::W,
            X => egui::Key::X,
            Y => egui::Key::Y,
            Z => egui::Key::Z,
            Num0 => egui::Key::Num0,
            Num1 => egui::Key::Num1,
            Num2 => egui::Key::Num2,
            Num3 => egui::Key::Num3,
            Num4 => egui::Key::Num4,
            Num5 => egui::Key::Num5,
            Num6 => egui::Key::Num6,
            Num7 => egui::Key::Num7,
            Num8 => egui::Key::Num8,
            Num9 => egui::Key::Num9,
            Delete => egui::Key::Delete,
            Escape => egui::Key::Escape,
            Return => egui::Key::Enter,
            Tab => egui::Key::Tab,
            Left => egui::Key::ArrowLeft,
            Right => egui::Key::ArrowRight,
            Up => egui::Key::ArrowUp,
            Down => egui::Key::ArrowDown,
            Space => egui::Key::Space,
            Backspace => egui::Key::Backspace,
            Apostrophe | Comma | BackApostrophe | Backslash | CapsLock | Command | Control
            | Equals | FrontSlash | LeftBracket | Minus | Option | Period | RightBracket
            | Semicolon | Shift | Delete => return None,
        };

        Some(key)
    }
}