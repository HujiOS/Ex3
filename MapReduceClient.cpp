//
// Created by Omer on 09/05/2017.
//

bool k1Base::operator==(const k3Base &other) const
{
    return this < other && other < this;
}

bool k2Base::operator==(const k2Base &other) const
{
    return this < other && other < this;
}

bool k3Base::operator==(const k3Base &other) const
{
    return this < other && other < this;
}


